import os
import zipfile
import fitparse
import csv
import tempfile
import shutil
import argparse
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial

def find_zip_files(root_dir):
    logging.info(f"Searching for ZIP files in '{root_dir}'")
    zip_files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.lower().endswith('.zip'):
                zip_path = os.path.join(dirpath, filename)
                zip_files.append(zip_path)
    logging.info(f"Found {len(zip_files)} ZIP files in '{root_dir}'")
    return zip_files

def find_fit_files(root_dir):
    logging.info(f"Searching for .fit files in '{root_dir}'")
    fit_files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.lower().endswith('.fit'):
                fit_path = os.path.join(dirpath, filename)
                fit_files.append(fit_path)
    logging.info(f"Found {len(fit_files)} .fit files in '{root_dir}'")
    return fit_files

def is_activity(fitfile):
    # Check if the file contains 'record' messages with 'timestamp's
    for record in fitfile.get_messages('record'):
        for data in record:
            if data.name == 'timestamp':
                return True  # Found a timestamp in a 'record' message
    return False

def extract_activity_data_from_file(fit_path, error_dir):
    try:
        fitfile = fitparse.FitFile(fit_path)
        if not is_activity(fitfile):
            logging.debug(f"File '{fit_path}' is not an activity file. Skipping.")
            return None

        summary = {}
        # Get session data
        for record in fitfile.get_messages('session'):
            for data in record:
                if data.name == 'start_time':
                    summary['start_time'] = data.value
                elif data.name == 'total_elapsed_time':
                    summary['total_time_sec'] = data.value
                elif data.name == 'total_distance':
                    summary['distance_km'] = data.value / 1000.0  # Convert meters to kilometers
                elif data.name == 'avg_heart_rate':
                    summary['avg_hr'] = data.value
                elif data.name == 'max_heart_rate':
                    summary['max_hr'] = data.value
                elif data.name == 'sport':
                    summary['sport'] = data.value
                elif data.name == 'sub_sport':
                    summary['sub_sport'] = data.value
                # Add other fields as needed

        # If 'start_time' is not found in 'session', try to find the minimum timestamp
        if 'start_time' not in summary:
            min_timestamp = None
            for record in fitfile.get_messages('record'):
                for data in record:
                    if data.name == 'timestamp':
                        if min_timestamp is None or data.value < min_timestamp:
                            min_timestamp = data.value
            if min_timestamp is not None:
                summary['start_time'] = min_timestamp

        if summary:
            summary['file_name'] = fit_path
            logging.debug(f"Extracted data from '{fit_path}'")
            return summary
        else:
            logging.debug(f"No summary data found in '{fit_path}'")
            return None
    except Exception as e:
        logging.error(f"Error processing file '{fit_path}': {e}")
        # Copy the problematic file to the error directory
        try:
            os.makedirs(error_dir, exist_ok=True)
            shutil.copy2(fit_path, error_dir)
            logging.debug(f"Copied '{fit_path}' to error directory '{error_dir}'")
        except Exception as copy_error:
            logging.error(f"Failed to copy '{fit_path}' to error directory: {copy_error}")
        return 'error'  # Return a special value to indicate an error

def process_zip_file(zip_path, temp_dir):
    try:
        logging.debug(f"Processing ZIP file '{zip_path}'")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        # Find .fit files in the extracted content
        fit_files = []
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.lower().endswith('.fit'):
                    fit_files.append(os.path.join(root, file))
        return fit_files
    except zipfile.BadZipFile:
        logging.warning(f"Skipping bad zip file '{zip_path}'")
        return []
    except Exception as e:
        logging.error(f"Error processing ZIP file '{zip_path}': {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description='Process Garmin .fit files into a CSV summary.')
    parser.add_argument('root_dir', help='Root directory containing .fit files or zip archives')
    parser.add_argument('-o', '--output', default='activities_summary.csv', help='Output CSV file name')
    parser.add_argument('-e', '--error_dir', default='error_files', help='Directory to store error files')
    parser.add_argument('-l', '--log', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    parser.add_argument('-w', '--workers', type=int, default=None, help='Number of worker processes (default: number of CPUs)')
    args = parser.parse_args()

    # Set up logging
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        print(f'Invalid log level: {args.log}')
        return
    logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s')

    root_dir = args.root_dir
    output_csv = args.output
    error_dir = args.error_dir  # Use the specified error directory

    # Use a temporary directory for unzipping files
    temp_dir = tempfile.mkdtemp()
    all_fit_files = []

    # Counters for summary
    total_files = 0
    successful_files = 0
    error_files = 0

    try:
        # Find and process ZIP files in parallel
        zip_files = find_zip_files(root_dir)
        logging.info(f"Processing {len(zip_files)} ZIP files...")
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(process_zip_file, zip_path, tempfile.mkdtemp(dir=temp_dir)): zip_path for zip_path in zip_files}
            for future in as_completed(futures):
                zip_path = futures[future]
                try:
                    fit_files = future.result()
                    all_fit_files.extend(fit_files)
                    logging.debug(f"Extracted {len(fit_files)} .fit files from '{zip_path}'")
                except Exception as e:
                    logging.error(f"Error processing ZIP file '{zip_path}': {e}")

        # Find .fit files directly in root_dir
        logging.info("Searching for .fit files in root directory...")
        all_fit_files.extend(find_fit_files(root_dir))

        total_files = len(all_fit_files)
        logging.info(f"Total .fit files to process: {total_files}")

        # Process .fit files in parallel
        activities = []
        processed_files = 0
        logging.info("Processing .fit files...")
        extract_func = partial(extract_activity_data_from_file, error_dir=error_dir)
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(extract_func, fit_path): fit_path for fit_path in all_fit_files}
            try:
                for future in as_completed(futures):
                    fit_path = futures[future]
                    processed_files += 1
                    logging.info(f"Processed file {processed_files}/{total_files}: '{fit_path}'")
                    data = future.result()
                    if data == 'error':
                        error_files += 1
                    elif data:
                        activities.append(data)
                        successful_files += 1
                        logging.debug(f"Extracted activity data from '{fit_path}'")
                    else:
                        logging.debug(f"No activity data found in '{fit_path}'")
                executor.shutdown(wait=True)
            except KeyboardInterrupt:
                logging.warning("Processing interrupted by user (Ctrl-C).")
                executor.shutdown(wait=False)
                # Remaining tasks are not completed

        # Write to CSV
        if activities:
            fieldnames = ['file_name', 'start_time', 'total_time_sec', 'distance_km',
                          'avg_hr', 'max_hr', 'sport', 'sub_sport']
            with open(output_csv, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for activity in activities:
                    writer.writerow(activity)
            logging.info(f"Data successfully written to '{output_csv}'")
        else:
            logging.warning("No activity data found in any files.")

        # Output summary
        logging.info("Processing complete.")
        logging.info(f"Total files processed: {total_files}")
        logging.info(f"Successful files: {successful_files}")
        logging.info(f"Files with errors: {error_files}")
        logging.info(f"Error files are copied to '{os.path.abspath(error_dir)}'")

    finally:
        # Clean up temp directory (excluding error_dir)
        shutil.rmtree(temp_dir)
        logging.info("Cleaned up temporary files.")

if __name__ == "__main__":
    main()
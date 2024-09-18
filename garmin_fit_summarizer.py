import os
import glob
import zipfile
import fitdecode
import csv
import tempfile
import shutil
import argparse
import logging
from fitdecode import FitReader, CrcCheck, ErrorHandling
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial

def find_zip_files(root_dir):
    logging.info(f"Searching for ZIP files in '{root_dir}'")
    zip_files = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        # Exclude '__MACOSX' directories
        dirnames[:] = [d for d in dirnames if not d.startswith('__MACOSX')]
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
        # Exclude '__MACOSX' directories
        dirnames[:] = [d for d in dirnames if not d.startswith('__MACOSX')]
        for filename in filenames:
            if filename.startswith('.'):
                continue  # Skip hidden files
            if filename.lower().endswith('.fit'):
                fit_path = os.path.join(dirpath, filename)
                fit_files.append(fit_path)
    logging.info(f"Found {len(fit_files)} .fit files in '{root_dir}'")
    return fit_files

def clear_error_directory(error_dir):
    if os.path.exists(error_dir):
        files = glob.glob(os.path.join(error_dir, '*'))
        for f in files:
            try:
                os.remove(f)
                logging.debug(f"Deleted file '{f}' from error directory.")
            except Exception as e:
                logging.error(f"Failed to delete file '{f}' from error directory: {e}", exc_info=True)

def extract_activity_data_from_file(fit_path, error_dir):
    try:
        # Use fitdecode to read the FIT file
        logging.debug(f"Processing file '{fit_path}'")
        try:
            fit_file = FitReader(
                fit_path,
                check_crc=CrcCheck.DISABLED,  # Use the CrcCheck enum
                error_handling=ErrorHandling.WARN  # Use the ErrorHandling enum
            )
        except Exception as e:
            logging.error(f"Error initializing FitReader for file '{fit_path}': {e}", exc_info=True)
            raise

        with fit_file as fit:
            sessions = []
            for frame in fit:
                if isinstance(frame, fitdecode.FitDataMessage):
                    try:
                        if frame.name == 'session':
                            # Start a new session summary
                            summary = {}
                            for field in frame.fields:
                                if field.name == 'start_time':
                                    summary['start_time'] = field.value
                                    logging.debug(f"Start time: {field.value}")
                                elif field.name == 'total_elapsed_time':
                                    total_elapsed_time = field.value if field.value is not None else 0
                                    summary['total_time_sec'] = total_elapsed_time
                                    logging.debug(f"Total elapsed time: {total_elapsed_time}")
                                elif field.name == 'total_distance':
                                    total_distance = field.value if field.value else 0
                                    summary['distance_km'] = total_distance / 1000.0  # Convert meters to kilometers
                                    logging.debug(f"Total distance: {total_distance}")
                                elif field.name == 'avg_heart_rate':
                                    summary['avg_hr'] = field.value
                                    logging.debug(f"Average heart rate: {field.value}")
                                elif field.name == 'max_heart_rate':
                                    summary['max_hr'] = field.value
                                    logging.debug(f"Max heart rate: {field.value}")
                                elif field.name == 'sport':
                                    sport = field.value.name if hasattr(field.value, 'name') else str(field.value)
                                    summary['sport'] = sport
                                    logging.debug(f"Sport: {sport}")
                                elif field.name == 'sub_sport':
                                    sub_sport = field.value.name if hasattr(field.value, 'name') else str(field.value)
                                    summary['sub_sport'] = sub_sport
                                    logging.debug(f"Sub Sport: {sub_sport}")
                            # Add file name to the summary
                            summary['file_name'] = fit_path
                            # Append the session summary to the list
                            sessions.append(summary)
                        # Additional logic for 'record' messages can be added here if needed
                    except Exception as e:
                        logging.warning(f"Error processing message '{frame.name}' in file '{fit_path}': {e}", exc_info=True)
                        continue

            if sessions:
                logging.debug(f"Extracted {len(sessions)} sessions from '{fit_path}'")
                return sessions
            else:
                logging.debug(f"No session data found in '{fit_path}'")
                return None

    except Exception as e:
        logging.error(f"Error processing file '{fit_path}': {e}", exc_info=True)
        # Copy the problematic file to the error directory
        try:
            os.makedirs(error_dir, exist_ok=True)
            shutil.copy2(fit_path, error_dir)
            logging.debug(f"Copied '{fit_path}' to error directory '{error_dir}'")
        except Exception as copy_error:
            logging.error(f"Failed to copy '{fit_path}' to error directory: {copy_error}", exc_info=True)
        return 'error'  # Return a special value to indicate an error

def process_zip_file(zip_path, temp_dir):
    try:
        logging.debug(f"Processing ZIP file '{zip_path}'")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract all files except those in '__MACOSX' directories or starting with '._'
            for member in zip_ref.namelist():
                if '__MACOSX' in member or os.path.basename(member).startswith('._'):
                    continue
                zip_ref.extract(member, temp_dir)
        # Find .fit files in the extracted content
        fit_files = []
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.startswith('.'):
                    continue  # Skip hidden files
                if file.lower().endswith('.fit'):
                    fit_files.append(os.path.join(root, file))
        return fit_files
    except zipfile.BadZipFile:
        logging.warning(f"Skipping bad zip file '{zip_path}'")
        return []
    except Exception as e:
        logging.error(f"Error processing ZIP file '{zip_path}': {e}", exc_info=True)
        return []

def main():
    parser = argparse.ArgumentParser(description='Process Garmin .fit files into a CSV summary.')
    parser.add_argument('root_dir', help='Root directory containing .fit files or zip archives')
    parser.add_argument('-o', '--output', default='activities_summary.csv', help='Output CSV file name')
    parser.add_argument('-e', '--error_dir', default='error_files', help='Directory to store error files')
    parser.add_argument('-l', '--log', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    parser.add_argument('-w', '--workers', type=int, default=os.cpu_count(), help='Number of worker processes (default: number of CPUs)')
    args = parser.parse_args()

    # Set up logging
    numeric_level = getattr(logging, args.log.upper(), None)
    if not isinstance(numeric_level, int):
        print(f'Invalid log level: {args.log}')
        return

    logger = logging.getLogger()
    logger.setLevel(numeric_level)

    # Remove all handlers associated with the root logger object
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    # Create file handler for logging errors and above
    file_handler = logging.FileHandler('debug.log', mode='w')
    file_handler.setLevel(logging.ERROR)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    root_dir = args.root_dir
    output_csv = args.output
    error_dir = args.error_dir
    num_workers = args.workers

    # Clear the error directory before processing
    clear_error_directory(error_dir)

    # Use a temporary directory for unzipping files
    temp_dir = tempfile.mkdtemp()
    all_fit_files = []

    # Counters for summary
    total_files = 0
    successful_files = 0
    error_files = 0

    try:
        # Find and process ZIP files
        zip_files = find_zip_files(root_dir)
        logging.info(f"Processing {len(zip_files)} ZIP files...")
        for zip_file in zip_files:
            zip_temp_dir = tempfile.mkdtemp(dir=temp_dir)
            fit_files = process_zip_file(zip_file, zip_temp_dir)
            all_fit_files.extend(fit_files)

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

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
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
                        if isinstance(data, list):
                            activities.extend(data)
                            successful_files += 1
                            logging.debug(f"Extracted {len(data)} activities from '{fit_path}'")
                        else:
                            activities.append(data)
                            successful_files += 1
                            logging.debug(f"Extracted activity data from '{fit_path}'")
                    else:
                        logging.debug(f"No activity data found in '{fit_path}'")
            except KeyboardInterrupt:
                logging.warning("Processing interrupted by user (Ctrl-C).")
                executor.shutdown(wait=False)
            except Exception as e:
                logging.error(f"An error occurred during multiprocessing: {e}", exc_info=True)
                executor.shutdown(wait=False)

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
        # Clean up temp directory
        shutil.rmtree(temp_dir)
        logging.info("Cleaned up temporary files.")

if __name__ == "__main__":
    main()
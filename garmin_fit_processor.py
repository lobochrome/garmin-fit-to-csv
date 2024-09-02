import os
import zipfile
import tempfile
import pandas as pd
from fitparse import FitFile
import argparse
import asyncio
import shutil
import logging
from datetime import datetime
from math import ceil

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def extract_min_timestamp(file_path, directory):
    try:
        fitfile = FitFile(file_path)
        min_timestamp = None

        for record in fitfile.get_messages(['record']):
            for data in record:
                if data.name == 'timestamp':
                    if min_timestamp is None or data.value < min_timestamp:
                        min_timestamp = data.value
        
        if min_timestamp is not None:
            return {"source_file": os.path.relpath(file_path, directory), "min_timestamp": min_timestamp}
        else:
            return None

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return None

async def process_fit_files(directory):
    fit_files = []
    results = []

    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.fit'):
                fit_files.append(os.path.join(root, filename))

    tasks = [extract_min_timestamp(file_path, directory) for file_path in fit_files]
    for result in await asyncio.gather(*tasks):
        if result:
            results.append(result)
    
    return results

async def unzip_and_process(directory):
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_files = []

        for root, _, files in os.walk(directory):
            for filename in files:
                if filename.endswith('.zip'):
                    zip_files.append(os.path.join(root, filename))

        for zip_path in zip_files:
            logger.info(f"Unzipping file: {zip_path}")
            await unzip_file(zip_path, temp_dir)
        
        return await process_fit_files(temp_dir)

async def unzip_file(zip_path, temp_dir):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
    except Exception as e:
        logger.error(f"Error unzipping {zip_path}: {e}")

def filter_and_copy(csv_file, zip_folder, output_folder, cutoff_date):
    logger.info("Starting filter and copy process")
    
    # Create the output folder if it doesn't exist
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Load the CSV file
    df = pd.read_csv(csv_file)

    # Convert 'min_timestamp' to datetime
    df['min_timestamp'] = pd.to_datetime(df['min_timestamp'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

    logger.info(f"Converted timestamps in CSV: {df['min_timestamp'].head()}")

    # Loop through each zip file in the folder
    for zip_filename in os.listdir(zip_folder):
        if zip_filename.endswith('.zip'):
            logger.info(f"Processing zip file: {zip_filename}")
            with zipfile.ZipFile(os.path.join(zip_folder, zip_filename), 'r') as zip_ref:
                # Extract the contents temporarily
                temp_folder = 'temp_extracted'
                zip_ref.extractall(temp_folder)
                logger.info(f"Extracted files to {temp_folder}")

                # Loop through each file in the extracted folder
                for root, dirs, files in os.walk(temp_folder):
                    for file in files:
                        file_path = os.path.join(root, file)
                        file_base_name = os.path.basename(file_path)
                        logger.info(f"Checking file: {file_base_name}")

                        # Check if the file is in the CSV and if its timestamp is before the cutoff date
                        if file_base_name in df['source_file'].values:
                            file_timestamp = df.loc[df['source_file'] == file_base_name, 'min_timestamp'].iloc[0]
                            logger.info(f"Original timestamp: {file_timestamp}")

                            if pd.notnull(file_timestamp):
                                logger.info(f"Valid datetime object: {file_timestamp}")
                                if file_timestamp < cutoff_date:
                                    logger.info(f"File {file_base_name} is older than the cutoff date. Copying to {output_folder}.")
                                    # Copy the file to the output folder
                                    shutil.copy(file_path, output_folder)
                                else:
                                    logger.info(f"File {file_base_name} is newer than the cutoff date. Skipping.")
                            else:
                                logger.warning(f"Invalid timestamp for file {file_base_name}. Skipping.")

                # Clean up the temporary folder
                shutil.rmtree(temp_folder)
                logger.info(f"Cleaned up temporary folder {temp_folder}")

    logger.info("Filter and copy process complete.")

def batch_files(source_folder, batch_size=25):
    logger.info("Starting batch files process")
    
    # Get all files in the source folder
    all_files = [f for f in os.listdir(source_folder) if os.path.isfile(os.path.join(source_folder, f))]
    
    # Calculate the number of batches needed
    num_batches = ceil(len(all_files) / batch_size)
    
    for batch_num in range(num_batches):
        # Create a new folder for this batch
        batch_folder = os.path.join(source_folder, f"batch_{batch_num + 1}")
        os.makedirs(batch_folder, exist_ok=True)
        
        # Get the files for this batch
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(all_files))
        batch_files = all_files[start_idx:end_idx]
        
        # Move files to the batch folder
        for file in batch_files:
            source_path = os.path.join(source_folder, file)
            dest_path = os.path.join(batch_folder, file)
            shutil.move(source_path, dest_path)
        
        logger.info(f"Created batch {batch_num + 1} with {len(batch_files)} files")

    logger.info("Batching process complete.")

async def main(args):
    logger.info("Starting main process")
    
    # Process FIT files and create CSV
    results = []
    results.extend(await unzip_and_process(args.input_folder))
    results.extend(await process_fit_files(args.input_folder))

    if results:
        # Convert the results to a DataFrame and save to CSV
        df = pd.DataFrame(results)
        df.to_csv(args.output_csv, index=False)
        logger.info(f"Data saved to {args.output_csv}")
    else:
        logger.warning("No data was extracted from the FIT files.")

    # Filter and copy files
    cutoff_date = pd.to_datetime(args.cutoff_date)
    filter_and_copy(args.output_csv, args.input_folder, args.output_folder, cutoff_date)

    # Batch files
    batch_files(args.output_folder, args.batch_size)

    logger.info("All processes completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Garmin FIT files, filter, and batch.")
    parser.add_argument('--input-folder', required=True, help="Directory containing FIT or ZIP files.")
    parser.add_argument('--output-csv', required=True, help="Path to save the output CSV file.")
    parser.add_argument('--output-folder', required=True, help="Directory to save filtered files.")
    parser.add_argument('--cutoff-date', required=True, help="Cutoff date for filtering (YYYY-MM-DD).")
    parser.add_argument('--batch-size', type=int, default=25, help="Number of files per batch (default: 25).")
    args = parser.parse_args()

    asyncio.run(main(args))
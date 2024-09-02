# Garmin Export FIT File Processing Script

This script processes Garmin FIT files as can be downloaded via their bulk data export, extracts timestamp information, filters files based on a cutoff date, and organizes them into batches that can then be uploaded to Strava. If you'd like to get your complete Garmin data history into Strava - this is "a" way.

Note this is umaintained and was written mostly by AI.

## Features

- Extracts minimum timestamps from Garmin FIT files
- Processes both individual FIT files and ZIP archives containing FIT files
- Filters files based on a specified cutoff date
- Organizes filtered files into batches
- Provides detailed logging for all operations

## Requirements

- Python 3.7+
- Required Python packages:
  - pandas
  - fitparse
  - asyncio

## Setup

It's recommended to use a virtual environment to manage dependencies for this project. Here's how you can set it up:

1. First, ensure you have Python 3.7 or higher installed on your system.

2. Clone this repository or download the script to your local machine.

3. Open a terminal and navigate to the project directory.

4. Create a virtual environment:
   ```
   python -m venv venv
   ```

5. Activate the virtual environment:
   - On Windows:
     ```
     venv\Scripts\activate
     ```
   - On macOS and Linux:
     ```
     source venv/bin/activate
     ```

6. Install the required packages:
   ```
   pip install pandas fitparse
   ```

Now you're ready to use the script within this isolated environment.

## Usage

```
python garmin_fit_processor.py --input-folder <input_folder> --output-csv <output_csv> --output-folder <output_folder> --cutoff-date <cutoff_date> [--batch-size <batch_size>]
```

### Arguments

- `--input-folder`: Directory containing FIT files or ZIP archives of FIT files
- `--output-csv`: Path to save the output CSV file containing extracted timestamp information
- `--output-folder`: Directory to save filtered files
- `--cutoff-date`: Cutoff date for filtering files (format: YYYY-MM-DD)
- `--batch-size`: (Optional) Number of files per batch (default: 25)

## Example

```
python garmin_fit_processor.py --input-folder /path/to/fit_files --output-csv /path/to/output.csv --output-folder /path/to/filtered_files --cutoff-date 2022-01-01 --batch-size 30
```

This command will:
1. Process all FIT files and ZIP archives in `/path/to/fit_files`
2. Save extracted timestamp information to `/path/to/output.csv`
3. Filter files older than January 1, 2022, and copy them to `/path/to/filtered_files`
4. Organize the filtered files into batches of 30 files each

## Output

1. A CSV file containing the source file names and their minimum timestamps
2. Filtered FIT files copied to the specified output folder
3. Batched folders containing the filtered files

## Logging

The script provides detailed logging information, which is printed to the console. You can redirect this output to a file if needed:

```
python garmin_fit_processor.py ... > logfile.txt 2>&1
```

## Notes

- Ensure you have sufficient disk space, especially when processing large ZIP archives.
- The script uses asynchronous operations for improved performance when processing multiple files.
- Make sure you have the necessary permissions to read from the input folder and write to the output folder and CSV file.
- When you're done using the script, you can deactivate the virtual environment by running:
  ```
  deactivate
  ```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
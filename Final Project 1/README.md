# JSON to CSV Transformer

This repository contains a script designed to transform JSON files containing data from Bitly and USA.gov into CSV files. The script extracts, cleans, and formats the data according to specified requirements.

## Problem Description

In 2012, Bitly partnered with USA.gov to provide anonymous data on shortened links ending with .gov or .mil. This data, stored in JSON format, includes various details such as the web browser, operating system, URLs, and timestamps.

### Data Fields

The JSON data includes the following key fields:

- `a`: Information about the web browser and operating system.
- `tz`: Time zone.
- `r`: URL the user came from.
- `u`: URL where the user headed to.
- `t`: Timestamp when the user started using the website (UNIX format).
- `hc`: Timestamp when the user exited the website (UNIX format).
- `cy`: The city from which the request was initiated.
- `ll`: Longitude and Latitude.

### Task Requirements

The script performs the following tasks:

1. **Transform JSON to DataFrame:** Converts JSON files into a DataFrame with specific columns.
2. **CSV Output:** Saves each transformed file as a CSV in a target directory.
3. **Data Cleaning:** Ensures that there are no NaN values and removes duplicates.
4. **Timestamp Handling:** Optionally keeps timestamps in UNIX format or converts them to human-readable format.
5. **Execution Time:** Prints the total execution time after processing.

### CSV Columns

The output CSV files will contain the following columns:

- `web_browser`: The web browser that requested the service.
- `operating_sys`: The operating system that initiated the request.
- `from_url`: The URL the user came from (shortened if necessary).
- `to_url`: The URL where the user headed to (shortened if necessary).
- `city`: The city from which the request was sent.
- `longitude`: The longitude of the request's origin.
- `latitude`: The latitude of the request's origin.
- `time_zone`: The time zone of the city.
- `time_in`: The time when the request started.
- `time_out`: The time when the request ended.

## Usage

### Prerequisites

- Python 3.8+
- Pandas library

### Script Parameters

The script accepts the following command-line arguments:

- `-i`: Input file path (directory containing JSON files).
- `-o`: Output file path (directory where CSV files will be saved).
- `-u`: Optional parameter; if passed, timestamps will be kept in UNIX format. If not passed, timestamps will be converted to human-readable format.

### Example Command

```bash
python transform_json_to_csv.py -i /path/to/json/files -o /path/to/output/files -u

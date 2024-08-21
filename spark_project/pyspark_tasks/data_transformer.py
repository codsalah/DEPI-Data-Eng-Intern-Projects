from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, from_unixtime
import sys
import argparse
import os
from datetime import datetime

def extract_browser(user_agent_string):
    web_browser_regex = r'(Mozilla|AppleWebKit|Chrome|Safari|Opera|MSIE|Trident)'
    return regexp_extract(user_agent_string, web_browser_regex, 1)

def extract_os(user_agent_string):
    os_regex = r'\(([^)]+)\)'
    return regexp_extract(user_agent_string, os_regex, 1)

def clean_url(url):
    pattern = r'(?:https?://)?(?:www\.)?([^\s/$.?#].[^\s]*)'
    return regexp_extract(col(url), pattern, 1)

def transform_data(input_path: str, output_path: str, unix_format: bool):
    if not os.path.exists(input_path):
        print(f"Input path '{input_path}' does not exist.")
        sys.exit(1)
    
    spark = SparkSession.builder \
        .appName("DataTransformer") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .getOrCreate()

    try:
        df = spark.read.json(input_path)
        
        # Print the schema and a few rows for debugging
        print("Initial DataFrame Schema:")
        df.printSchema()
        print("Initial DataFrame Sample Rows:")
        df.show(5, truncate=False)

        # Extract web browser and operating system
        df = df.withColumn('web_browser', extract_browser(col('a')))
        df = df.withColumn('operating_sys', extract_os(col('a')))

        # Clean URLs
        df = df.withColumn('from_url', clean_url('r'))
        df = df.withColumn('to_url', clean_url('u'))

        # Separate longitude and latitude from the array column 'll'
        df = df.withColumn('longitude', col('ll').getItem(1).cast('double'))
        df = df.withColumn('latitude', col('ll').getItem(0).cast('double'))
        df = df.drop('ll')  # Drop the original 'll' column

        # City info
        df = df.withColumn('city', col('cy'))
        df = df.withColumn('time_zone', col('tz'))

        # Handle timestamps
        if unix_format:
            df = df.withColumn('time_in', col('t'))
            df = df.withColumn('time_out', col('hc'))
        else:
            df = df.withColumn('time_in', from_unixtime(col('t')))
            df = df.withColumn('time_out', from_unixtime(col('hc')))

        # Drop rows with any null values
        df = df.na.drop()

        # Print the schema and a few rows after transformation
        print("Transformed DataFrame Schema:")
        df.printSchema()
        print("Transformed DataFrame Sample Rows:")
        df.show(5, truncate=False)

        # Create a unique subdirectory within the output path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_subdir = os.path.join(output_path, f"output_{timestamp}")

        # Ensure the subdirectory exists
        os.makedirs(output_subdir, exist_ok=True)

        # Save to CSV in the subdirectory
        df.write.csv(output_subdir, header=True, mode='overwrite')

        # Print the number of records
        record_count = df.count()
        print(f"Number of records: {record_count}")

        # Print message with number of rows transformed and file's path
        print(f"CSV file created at {output_subdir} with {record_count} rows.")

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

    finally:
        # Stop the SparkSession
        spark.stop()

if __name__ == "__main__":

    # Absolute paths for testing purposes
    default_args = [
        "-i", "D:/github repos/Data-Warehouse-Projects/spark_project/Data/usa.gov_click_data.json", 
        "-o", "D:/github repos/Data-Warehouse-Projects/spark_project/Output/"
    ]

    parser = argparse.ArgumentParser(description="Transform JSON data to CSV")
    parser.add_argument("-i", "--input", required=True, help="Input file path")
    parser.add_argument("-o", "--output", required=True, help="Output directory path")
    parser.add_argument("-u", "--unix", action="store_true", help="Keep timestamps in UNIX format")
    
    args = parser.parse_args(default_args)  
    
    transform_data(args.input, args.output, args.unix)

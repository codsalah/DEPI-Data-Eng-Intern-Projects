from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_extract, from_unixtime
from pyspark.sql.types import DoubleType
import time

def clean_and_transform(df, unix_format=False):
    """
    Transforms the input DataFrame according to the specified requirements.

    Parameters:
    df (DataFrame): The input DataFrame containing the JSON data.
    unix_format (bool): If True, keeps timestamps in UNIX format; otherwise, converts to human-readable format.

    Returns:
    DataFrame: The transformed DataFrame.
    """
    # Extract web browser and operating system
    df = df.withColumn('web_browser', split(col('a'), ' ').getItem(0))
    df = df.withColumn('operating_sys', split(col('a'), ' ').getItem(1))
    
    # Clean URLs
    df = df.withColumn('from_url', regexp_extract(col('r'), r'(?:https?://)?(?:www\.)?([^\s/$.?#].[^\s]*)', 1))
    df = df.withColumn('to_url', regexp_extract(col('u'), r'(?:https?://)?(?:www\.)?([^\s/$.?#].[^\s]*)', 1))
    
    # Extract and clean additional columns
    df = df.withColumn('city', col('cy'))
    df = df.withColumn('longitude', col('ll').getItem(0).cast(DoubleType()))
    df = df.withColumn('latitude', col('ll').getItem(1).cast(DoubleType()))
    df = df.withColumn('time_zone', col('tz'))
    
    # Handle timestamps
    if unix_format:
        df = df.withColumn('time_in', col('t'))
        df = df.withColumn('time_out', col('hc'))
    else:
        df = df.withColumn('time_in', from_unixtime(col('t')))
        df = df.withColumn('time_out', from_unixtime(col('hc')))
    
    # Drop unwanted columns and handle missing values
    df = df.drop('a', 'tz', 'r', 'u', 't', 'hc', 'cy', 'll')
    df = df.dropna()
    df = df.dropDuplicates()
    
    return df

def main():
    start_time = time.time()
    
    # Initialize Spark session
    spark = SparkSession.builder.master("local[*]").appName("JSON Transformation").getOrCreate()
    
    # Example JSON data
    json_data = '''
    [
      {
        "a": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.78 Safari/535.11",
        "c": "US",
        "nk": 1,
        "tz": "America/New_York",
        "gr": "MA",
        "g": "A6qOVH",
        "h": "wfLQtf",
        "l": "orofrog",
        "al": "en-US,en;q=0.8",
        "hh": "1.usa.gov",
        "r": "http://www.facebook.com/l/7AQEFzjSi/1.usa.gov/wfLQtf",
        "u": "http://www.ncbi.nlm.nih.gov/pubmed/22415991",
        "t": 1333307030,
        "hc": 1333307037,
        "cy": "Danvers",
        "ll": [42.576698, -70.954903]
      }
    ]
    '''
    
    # Convert JSON string to DataFrame
    rdd = spark.sparkContext.parallelize([json_data])
    df = spark.read.json(rdd)
    
    # Transform the data
    transformed_df = clean_and_transform(df, unix_format=False)  # Set unix_format=True if needed
    
    # Print the number of records
    record_count = transformed_df.count()
    print(f"Number of records: {record_count}")
    
    # Save the DataFrame to a CSV file
    output_file = 'transformed_data.csv'
    transformed_df.write.csv(output_file, header=True, mode='overwrite')
    
    # Print a message after saving
    print(f"Data transformed and saved to {output_file} with {record_count} rows.")
    
    # Print the total execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Total execution time: {execution_time:.2f} seconds")
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()

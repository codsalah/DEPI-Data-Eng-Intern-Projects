# pyspark_tasks/spark_exploration.py

from pyspark.sql import SparkSession
import os

def explore_json(file_path: str):
    """
    Explore a JSON file by reading it into a Spark DataFrame and displaying various details.
    
    :param file_path: Path to the JSON file.
    """
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("SparkExploration") \
        .getOrCreate()
    
    # Read JSON file
    df = spark.read.json(file_path)
    
    # Show the schema of the DataFrame
    print("Schema of the DataFrame:")
    df.printSchema()
    
    # Show the first few rows of the DataFrame
    print("\nFirst few rows of the DataFrame:")
    df.show()
    
    # Show basic statistics
    print("\nBasic statistics:")
    df.describe().show()

if __name__ == "__main__":
    # Construct the absolute path to the JSON file
    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Data', 'usa.gov_click_data.json'))
    explore_json(file_path)


'''
Schema of the DataFrame:
root
 |-- a: string (nullable = true)
 |-- al: string (nullable = true)
 |-- c: string (nullable = true)
 |-- cy: string (nullable = true)
 |-- g: string (nullable = true)
 |-- gr: string (nullable = true)
 |-- h: string (nullable = true)
 |-- hc: long (nullable = true)
 |-- hh: string (nullable = true)
 |-- l: string (nullable = true)
 |-- ll: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- nk: long (nullable = true)
 |-- r: string (nullable = true)
 |-- t: long (nullable = true)
 |-- tz: string (nullable = true)
 |-- u: string (nullable = true)

proper structure
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

Timestamp 1333307030 (UNIX timestamp):
This corresponds to: 2012-04-01 05:23:50 (UTC)

Timestamp 1333307037 (UNIX timestamp):
This corresponds to: 2012-04-01 05:23:57 (UTC)

yyyy-MM-dd HH:mm:ss

'''
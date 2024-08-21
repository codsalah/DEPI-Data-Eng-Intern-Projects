import sys
import os
import shutil
import json
from pyspark.sql import SparkSession

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark_tasks.data_transformer import transform_data

def setup_module():
    """Setup temporary directories and sample data for testing."""
    os.makedirs('test_data', exist_ok=True)
    os.makedirs('test_output', exist_ok=True)

    sample_json = [
        {
            "a": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
            "c": "US",
            "nk": 1,
            "tz": "America/New_York",
            "gr": "NY",
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

    with open('test_data/sample.json', 'w') as f:
        json.dump(sample_json, f)

def teardown_module():
    """Cleanup temporary directories after testing."""
    if os.path.exists('test_data'):
        shutil.rmtree('test_data')
    if os.path.exists('test_output'):
        shutil.rmtree('test_output')

def test_transform_data():
    try:
        setup_module()

        # Run the transformation
        transform_data('test_data/sample.json', 'test_output/transformed_data.csv', unix_format=False)

        # Check if the output CSV file exists
        assert os.path.exists('test_output/transformed_data.csv'), "CSV file was not created."

        # Check if the CSV file contains data
        spark = SparkSession.builder \
            .appName("TestTransformData") \
            .getOrCreate()

        df = spark.read.csv('test_output/transformed_data.csv', header=True, inferSchema=True)
        assert df.count() > 0, "CSV file is empty."

        # Validate columns
        expected_columns = {'web_browser', 'operating_sys', 'from_url', 'to_url', 'city', 'longitude', 'latitude', 'time_zone', 'time_in', 'time_out'}
        actual_columns = set(df.columns)
        assert expected_columns == actual_columns, f"Columns do not match. Expected {expected_columns}, got {actual_columns}"

    finally:
        teardown_module()

if __name__ == "__main__":
    test_transform_data()
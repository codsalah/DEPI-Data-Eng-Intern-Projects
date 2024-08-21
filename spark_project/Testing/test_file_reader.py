import sys
import os
from pyspark.sql import SparkSession

# Add the root directory to PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark_tasks.file_reader import read_json

def test_read_json():
    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'Data', 'usa.gov_click_data.json'))
    df = read_json(file_path)
    
    # Show the first few rows of the DataFrame to verify it's working
    df.show()

if __name__ == "__main__":
    test_read_json()
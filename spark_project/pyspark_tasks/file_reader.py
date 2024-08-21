from pyspark.sql import SparkSession

def read_json(file_path: str):
    """
    Reads a JSON file and returns a Spark DataFrame.
    
    :param file_path: Path to the JSON file.
    :return: Spark DataFrame containing the data from the JSON file.
    """
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("FileReader") \
        .getOrCreate()
    
    # Read JSON file
    df = spark.read.json(file_path)
    
    return df


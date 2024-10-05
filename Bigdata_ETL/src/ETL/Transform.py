import sys,os
from pyspark.sql import SparkSession
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from src.ETL.ETL import ETL

def data_frame_generator():
    etl = ETL(db_name='CustumerSegementation', user='postgres', password='Nish123@#@')

    # Read data from PostgreSQL
    data = etl.read_data()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PostgreSQL to PySpark") \
        .getOrCreate()

    # Create a PySpark DataFrame from the Pandas DataFrame
    spark_df = spark.createDataFrame(data)
    
    return spark_df
main_df=data_frame_generator()
main_df.show(5,truncate=True)
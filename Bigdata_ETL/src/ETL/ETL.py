import pandas as pd
import sys
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, DateType
from psycopg2 import sql

class ETL:
    def __init__(self, db_name, user, password, host='localhost', port='5432'):
        self.db_name = db_name
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None

    def sql_conn(self):
        """Create a connection to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print("Database connection established.")
            return self.connection
        except Exception as e:
            print(f"Error connecting to database: {e}")
            sys.exit(1)

    def read_data(self):
        """Read data from PostgreSQL table and return it as a Pandas DataFrame."""
        if self.connection is None:
            self.sql_conn()  # Ensure connection is established

        try:
            query = "SELECT * FROM my_table;"
            data = pd.read_sql(query, self.connection)
            print("Data read successfully.")
            return data
        except Exception as e:
            print(f"Error reading data: {e}")
        finally:
            self.close_connection()

    def close_connection(self):
        """Close the database connection."""
        if self.connection is not None:
            self.connection.close()
            print("Database connection closed.")

def main():
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

# if __name__ == "__main__":
#     main()

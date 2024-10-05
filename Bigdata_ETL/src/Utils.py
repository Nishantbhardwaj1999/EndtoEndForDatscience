import pandas as pd
import sys
import os
import psycopg2
from psycopg2 import sql
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.logger import logging

class Utils:
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
            logging.info("Database connection established.")
            return self.connection
        except Exception as e:
            logging.error(f"Error connecting to database: {e}")
            sys.exit(1)

    def create_table(self):
        """Create a table in the database."""
        if self.connection is None:
            self.sql_conn()  # Ensure connection is established

        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS my_table (
                    ID SERIAL PRIMARY KEY,
                    Year_Birth BIGINT,
                    Education VARCHAR(100),
                    Marital_Status VARCHAR(50),
                    Income BIGINT,
                    Kidhome INT,
                    Teenhome INT,
                    Dt_Customer DATE,
                    Recency INT,
                    MntWines BIGINT,
                    MntFruits BIGINT,
                    MntMeatProducts BIGINT,
                    MntFishProducts BIGINT,
                    MntSweetProducts BIGINT,
                    MntGoldProds BIGINT,
                    NumDealsPurchases INT,
                    NumWebPurchases INT,
                    NumCatalogPurchases INT,
                    NumStorePurchases INT,
                    NumWebVisitsMonth INT,
                    AcceptedCmp3 INT,
                    AcceptedCmp4 INT,
                    AcceptedCmp5 INT,
                    AcceptedCmp1 INT,
                    AcceptedCmp2 INT,
                    Complain INT,
                    Z_CostContact BIGINT,
                    Z_Revenue BIGINT,
                    Response INT
                )
            """)
            self.connection.commit()
            logging.info("Table 'my_table' created successfully.")
        except Exception as e:
            logging.error(f"Error creating table: {e}")
        finally:
            cursor.close()

    def insert_data(self, data):
        """Insert data into the database table."""
        if self.connection is None:
            self.sql_conn()  # Ensure connection is established

        try:
            cursor = self.connection.cursor()

            # Check DataFrame shape and columns
            logging.info(f"DataFrame shape: {data.shape}")
            logging.info(f"DataFrame columns: {data.columns.tolist()}")

            # Convert the 'Dt_Customer' column to the correct format
            data['Dt_Customer'] = pd.to_datetime(data['Dt_Customer'], format='%d-%m-%Y', errors='coerce').dt.strftime('%Y-%m-%d')

            # Fill nan values or drop rows with missing values
            data.fillna({'Income': 0, 'MntWines': 0, 'MntFruits': 0, 'MntMeatProducts': 0, 
                        'MntFishProducts': 0, 'MntSweetProducts': 0, 'MntGoldProds': 0}, inplace=True)

            # Check for out of range values for integer columns
            data = data[(data['Income'] >= 0) & 
                        (data['MntWines'].between(0, 100000)) &  # Adjust range as needed
                        (data['MntFruits'].between(0, 100000)) &
                        (data['MntMeatProducts'].between(0, 100000)) &
                        (data['MntFishProducts'].between(0, 100000)) &
                        (data['MntSweetProducts'].between(0, 100000)) &
                        (data['MntGoldProds'].between(0, 100000))]

            insert_query = """
                INSERT INTO my_table (
                    Year_Birth, Education, Marital_Status, Income, 
                    Kidhome, Teenhome, Dt_Customer, Recency, MntWines, 
                    MntFruits, MntMeatProducts, MntFishProducts, 
                    MntSweetProducts, MntGoldProds, NumDealsPurchases, 
                    NumWebPurchases, NumCatalogPurchases, NumStorePurchases, 
                    NumWebVisitsMonth, AcceptedCmp3, AcceptedCmp4, 
                    AcceptedCmp5, AcceptedCmp1, AcceptedCmp2, Complain, 
                    Z_CostContact, Z_Revenue, Response
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for row in data.itertuples(index=False):
                row_data = row[1:] if len(row) > 1 else row
                logging.info(f"Inserting row data: {row_data}")

                cursor.execute(insert_query, row_data)

            self.connection.commit()
            logging.info("Data inserted successfully.")
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
            self.connection.rollback()  # Roll back the transaction if an error occurs
        finally:
            cursor.close()

    def close_connection(self):
        """Close the database connection."""
        if self.connection is not None:
            self.connection.close()
            logging.info("Database connection closed.")

# if __name__ == "__main__":
#     utils = Utils(db_name='CustumerSegementation', user='postgres', password='Nish123@#@')
    
#     # Connect to the database
#     utils.sql_conn()
    
#     # Create the table
#     utils.create_table()

#     # Load data from CSV
#     csv_file_path = 'DataSet/customer_segmentation.csv'
#     data = pd.read_csv(csv_file_path)

#     # Insert data into the database
#     utils.insert_data(data)

#     # Close the connection
#     utils.close_connection()

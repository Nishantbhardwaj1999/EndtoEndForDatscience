import sys,os
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from src.Utils import Utils

if __name__ == "__main__":
    utils = Utils(db_name='CustumerSegementation', user='postgres', password='Nish123@#@')
    
    utils.sql_conn()
    

    utils.create_table()


    csv_file_path = 'DataSet/customer_segmentation.csv'
    data = pd.read_csv(csv_file_path)


    utils.insert_data(data)


    utils.close_connection()


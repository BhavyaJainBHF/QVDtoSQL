import pyodbc
import pandas as pd
from qvd import qvd_reader as QVDReader
import logging
import json
from datetime import datetime, timezone
from sqlalchemy import create_engine
import os
#from utils.config import errorLog
#from utils.func_utils import addLogs, create_todays_file, download_files, download_page_handler, fetchData, json_handler, move_to_archive, random_delay, retry_handler as retry, url, handle_download_page


def LoadDataFromQvdToSql():
    print('Inside function LoadDataFromQvdToSql')
    logging.info('Inside function LoadDataFromQvdToSql')
    # Read QVD file into pandas DataFrame
    #qvd_path = 'C:/Amazon_Forecast_Factor.qvd'
    qvd_path = connection["QVDFilePath"]
    qvd_data = QVDReader.read(connection["QVDFilePath"])
    #qvd_data = QVDReader(qvd_path).to_dataframe()

    # Connect to SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server='+connection["Server"]+';'
                           'Database='+connection["Database"]+';'
                           'UID='+connection["UID"]+';'
                           'PWD='+connection["PWD"]+';')

    
    cursor = conn.cursor()
    #cursor.fast_executemany = True
    print(('DB Connection established'))
    logging.info('DB Connection established')

    # Insert DataFrame into SQL Table
    for index, row in qvd_data.iterrows():
        print('Reading info from row with Item'+row['BHF_Item'])
        logging.info('Reading info from row with Item'+row['BHF_Item'])
        cursor.execute('''
            INSERT INTO QVD_AmazonForecastFactor ([BHF_Item], [ItemDescription], [Size], [Color], [Program], [Brand], [Category], [MasterDescription], [Sales_2024_YTD], [Forecast_p80_2024_YTD],
            [Forecast_p70_2024_YTD], [Forecast_p90_2024_YTD], [Forecast_91d_p80_2024_YTD], [Forecast_4M_p80_2024_YTD], [Factor_p80], [Factor_p70], [Factor_p90], [Factor_91d_p80], [Factor_4M_p80],
            [Weighted_12W_Sales], [Weighted_12W_p80_units], [Weighted_12W_Factor_p80])
            VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', row['BHF_Item'], row['ItemDescription'], row['Size'], row['Color'], row['Program'], row['Brand'], row['Category'], row['MasterDescription'], row['Sales_2024_YTD'], row['Forecast_p80_2024_YTD'],
            row['Forecast_p70_2024_YTD'], row['Forecast_p90_2024_YTD'], row['Forecast_91d_p80_2024_YTD'], row['Forecast_4M_p80_2024_YTD'], row['Factor_p80'], row['Factor_p70'], row['Factor_p90'], row['Factor_91d_p80'], row['Factor_4M_p80'],
            row['Weighted_12W_Sales'], row['Weighted_12W_p80_units'], row['Weighted_12W_Factor_p80'])

    conn.commit()
    cursor.close()
    conn.close()

    print('Execution finished')
    logging.info('Execution finished')


# <-------------------STARTING POINT------------------->

with open('connection.json') as f:
   connection = json.load(f)

log_directory = connection["LogFilePath"] 

logging.basicConfig(
        filename=os.path.join(log_directory),
        level=logging.INFO
    )

print('Started execution')
logging.info('Started execution')



LoadDataFromQvdToSql()
  
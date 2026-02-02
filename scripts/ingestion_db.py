import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
    # filename="logs/ingestion_db.log",
    filename=r"C:\Projects\Vendor_Performance_Analysis-SQL-Python-Powerbi\logs\ingestion_db.log",

    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s -%(message)s",
    filemode="a"
)

engine = create_engine(
    r"sqlite:///C:\Projects\Vendor_Performance_Analysis-SQL-Python-Powerbi\Inventory.db"
)


def ingest_db(df, table_name, engine):
    '''This function will injust the dataframe into database table'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=100000)


def load_raw_data():
    '''This function will load the CSVs as dataframe and adjust into db'''
    start = time.time()
    for file in os.listdir(r"C:\Projects\Vendor_Performance_Analysis-SQL-Python-Powerbi\data"):
        if '.csv' in file:
            df = pd.read_csv( r"C:\Projects\Vendor_Performance_Analysis-SQL-Python-Powerbi\data\\" + file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start) / 60

    logging.info("Ingestion complete")
    logging.info(f"\nTotal Time Taken : {total_time} minutes")


if __name__ == '__main__':
    load_raw_data()
    logging.shutdown()
import pandas as pd
from sqlalchemy import create_engine 
from time import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    gz_name = 'output.csv.gz'
    csv_name = 'output.csv'
    os.system(f"wget {url} -O {gz_name}")
    os.system(f"gzip -d {gz_name}")
    df = pd.read_csv(csv_name, nrows=100)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize = 100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name = table_name, con=engine, if_exists='append')
    while True:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        print('insert another chunk...')
        t_end = time()
        print(f'inserted another chunk... took {t_end-t_start} seconds')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')
    parser.add_argument('--user', help = 'users for postgres')
    parser.add_argument('--password', help = 'pass for postgres')
    parser.add_argument('--host', help = 'host for postgres')
    parser.add_argument('--port', help = 'port for postgres')
    parser.add_argument('--db', help = 'db for postgres')
    parser.add_argument('--table_name', help = 'table for postgres')
    parser.add_argument('--url', help = 'url for postgres')

    args = parser.parse_args()
    main(args)
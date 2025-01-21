import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
from dotenv import load_dotenv


def main():
    load_dotenv()

    pg_user = os.getenv('PG_USER')
    password = os.getenv('PASSWORD')
    host= os.getenv('HOST')
    db = os.getenv('DB')
    port = os.getenv('PORT')
    table1_name = os.getenv('TABLE1_NAME')
    url1 = os.getenv('URL1')
    url2 = os.getenv('URL2')

    csv_name1 = 'yellow_tripdata_csv'
    csv_name2 = 'zone_lookup_csv'
    directory = './csv_data'

    if not os.path.exists(directory):
        os.makedirs(directory)
        os.system(f"wget {url1} -O ./csv_data/{csv_name1}")
        os.system(f"wget {url2} -O ./csv_data/{csv_name2}")

    engine = create_engine(f'postgresql://{pg_user}:{password}@{host}:{port}/{db}')

    engine.connect()

    df_iter = pd.read_csv("yellow_tripdata_2021-01.csv.gz", iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)



    # Create and replace tables
    df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
    df.head(n=0).to_sql(name='zone_lookup', con=engine, if_exists='replace')


    df.to_sql(name=table1_name, con=engine, if_exists='append')


    try:
        while True:
            t_start = time()
            
            df = next(df_iter)
            
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

            t_end = time()

            print("Inserted another chunk of rows, time to complete: %.2f" % (t_end - t_start))

    except StopIteration:
        print("All rows have been uploaded successfully")

    try:
        time_start = time()

        df.to_sql(name='zone_lookup', con=engine, if_exists='append', method='multi')

        time_end = time()

        print("Inserted all of zone_lookup rows, time to complete: %.2f" % (time_end - time_start))
    except Exception as e:
        print(e)

    print('Extract phase complete')

if __name__ == '__main__':
    main()
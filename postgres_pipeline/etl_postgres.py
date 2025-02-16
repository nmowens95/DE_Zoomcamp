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
    table2_name = os.getenv('TABLE2_NAME')
    url1 = os.getenv('URL1')
    url2 = os.getenv('URL2')

    csv_name1 = 'yellow_tripdata'
    parquet_name = 'green_tripdata'
    directory = './csv_data' 

    # Define file paths
    taxi_csv_download = f"{directory}/{csv_name1}.csv"
    green_parquet_download = f"{directory}/{parquet_name}.parquet"

    # Create a directory if it doesn't exist
    if not os.path.exists(directory):
        os.makedirs(directory)

    ''' Download only if the files do not exist '''

    # Yellow Taxi Data file Verification
    if not os.path.exists(taxi_csv_download):
        print(f"Downloading {csv_name1}.csv...")
        os.system(f"wget {url1} -O {directory}/{csv_name1}.csv")
    else:
        print(f"{csv_name1}.csv already exists. Skipping download.")

    # Green Taxi Data file verification
    if not os.path.exists(green_parquet_download):
        print(f"Downloading {parquet_name}.parquet...")
        os.system(f"wget {url2} -O {directory}/{parquet_name}.parquet")
    else:
        print(f"{parquet_name}.parquet already exists. Skipping download.")


    # Create DB Connections
    engine = create_engine(f'postgresql://{pg_user}:{password}@{host}:{port}/{db}')

    engine.connect()


    # path to file
    # file_path = os.path.join(os.getcwd(), "yellow_tripdata_2021-01.csv")
    taxi_csv = "/opt/airflow/postgres_pipeline/yellow_tripdata_2021-01.csv"
    green_parquet = green_parquet_download

    # Create iterator to upload it chunks
    df_iter_yellow = pd.read_csv(taxi_csv, iterator=True, chunksize=100000)
    
    df_green = pd.read_parquet(green_parquet)
    df_yellow = next(df_iter_yellow)

    # Convert to datetime
    df_yellow.tpep_pickup_datetime = pd.to_datetime(df_yellow.tpep_pickup_datetime)
    df_yellow.tpep_dropoff_datetime = pd.to_datetime(df_yellow.tpep_dropoff_datetime)

    # Standardize and lowercase columns
    df_yellow.columns = [col.lower() for col in df_yellow.columns]
    df_green.columns = [col.lower() for col in df_green.columns]


    # Create and replace tables
    df_yellow.head(n=0).to_sql(name=table1_name, con=engine, if_exists='replace')
    df_green.head(n=0).to_sql(name=table2_name, con=engine, if_exists='replace')


    df_yellow.to_sql(name=table1_name, con=engine, if_exists='append')
    df_green.to_sql(name=table2_name, con=engine, if_exists='append')

    # Green taxi data upload
    try:
        time_start = time()

        df_green.columns = [col.lower() for col in df_green.columns]
        df_green.to_sql(name=table2_name, con=engine, if_exists='append')

        time_end = time()

        print(f"Inserted all of {table2_name} rows, time to complete: %.2f" % (time_end - time_start))
    except Exception as e:
        print(e)

    # Yellow taxi data upload
    try:
        while True:
            t_start = time()
            
            df_yellow = next(df_iter_yellow)
            
            df_yellow.tpep_pickup_datetime = pd.to_datetime(df_yellow.tpep_pickup_datetime)
            df_yellow.tpep_dropoff_datetime = pd.to_datetime(df_yellow.tpep_dropoff_datetime)

            df_yellow.columns = [col.lower() for col in df_yellow.columns]

            df_yellow.to_sql(name=table1_name, con=engine, if_exists='append')

            t_end = time()

            print("Inserted another chunk of rows, time to complete: %.2f" % (t_end - t_start))

    except StopIteration:
        print("All rows have been uploaded successfully")

    print('Extract phase complete')

if __name__ == '__main__':
    main()
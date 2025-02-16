import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# PostgreSQL connection settings
USER = os.getenv("PG_USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DB = os.getenv("DB")

# Local storage settings
# CLEANED_DIR = Path("./data")
data_dir = './csv_data'
TABLE_NAME = "yellow_taxi_pickup"

# Create a directory if it doesn't exist
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
else:
    print(f"{data_dir} already exists.")

# Connect to PostgreSQL
engine = create_engine(f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

# Define SQL query to fetch data
QUERY = f"SELECT * FROM {TABLE_NAME};"

print("📥 Extracting data from PostgreSQL...found")
df = pd.read_sql(QUERY, engine)

# Log the number of rows fetched from PostgreSQL
num_rows_extracted = df.shape[0]
print(f"✅ Extracted {num_rows_extracted} rows from PostgreSQL.")


### **🔹 Data Cleaning Steps** ###

# **1️⃣ Rename columns to match BigQuery schema**
df.rename(columns={
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "ratecodeid": "rate_code_id",
    "pulocationid": "pickup_location_id",
    "dolocationid": "dropoff_location_id",
}, inplace=True)

# **2️⃣ Drop unwanted columns**
df.drop(columns=["congestion_surcharge", "airport_fee"], inplace=True, errors="ignore")

# **3️⃣ Convert timestamps to timezone-aware UTC**
df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce", utc=True)
df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce", utc=True)

# **4️⃣ Remove rows where pickup or dropoff is NaT (invalid)**
df.dropna(subset=["pickup_datetime", "dropoff_datetime"], inplace=True)

# **5️⃣ Clamp timestamps to BigQuery-compatible range** 
#    (year 0001 to 9999)
min_bq_datetime = pd.Timestamp(year=1, month=1, day=1, tz='UTC')
max_bq_datetime = pd.Timestamp(year=9999, month=12, day=31, tz='UTC')
df = df[
    (df["pickup_datetime"] >= min_bq_datetime) &
    (df["pickup_datetime"] <= max_bq_datetime) &
    (df["dropoff_datetime"] >= min_bq_datetime) &
    (df["dropoff_datetime"] <= max_bq_datetime)
]

# **6️⃣ Convert timezone-aware to naive & cast to microseconds**
#    This ensures that nanosecond timestamps won't exceed BigQuery limits.
df["pickup_datetime"] = df["pickup_datetime"].dt.tz_convert(None).astype("datetime64[us]")
df["dropoff_datetime"] = df["dropoff_datetime"].dt.tz_convert(None).astype("datetime64[us]")

# **7️⃣ Ensure correct data types**
df["passenger_count"] = df["passenger_count"].fillna(0).astype(int)
df["rate_code_id"] = df["rate_code_id"].fillna(0).astype(int)

# **8️⃣ Drop duplicates**
df.drop_duplicates(inplace=True)

# **9️⃣ Save cleaned data to Parquet**
parquet_file_path = f"{data_dir}/{TABLE_NAME}_cleaned.parquet"
df.to_parquet(f"{data_dir}/{TABLE_NAME}_cleaned.parquet", engine="pyarrow", index=False)

# Log the number of rows saved to the Parquet file
num_rows_saved = df.shape[0]
print(f"✅ Successfully saved {num_rows_saved} rows to {parquet_file_path}")

# Check if there was a row count mismatch
if num_rows_extracted != num_rows_saved:
    print(f"⚠️ Warning: {num_rows_extracted - num_rows_saved}")
else: 
    print(f"⚠️ Warning: {num_rows_extracted - num_rows_saved} rows were dropped during cleaning!")

print(f"✅ Cleaned data saved to {data_dir}/{TABLE_NAME}.parquet")
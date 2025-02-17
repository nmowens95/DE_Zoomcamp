from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import os

GCS_KEY_PATH = "/opt/airflow/keys/ny-rides-nate.json"


# Define GCS file and BigQuery table details
GCS_BUCKET_NAME = "ny-rides-nate"
PROJECT_ID = "ny-rides-nate"
BQ_DATASET_NAME = "ny_taxi"  # Matches Terraform dataset name

BQ_TABLE_NAME = "yellow_taxi_pickup"
BQ_TABLE_NAME2 = "green_taxi_pickup"
BQ_TABLE_NAME3 = "zone_lookup"

GCS_FILE_URI = f"gs://{GCS_BUCKET_NAME}/yellow_taxi_pickup_cleaned.parquet"
GCS_FILE_URI2= f"gs://{GCS_BUCKET_NAME}/green_tripdata.parquet"
GCS_FILE_URI3= f"gs://{GCS_BUCKET_NAME}/zone_lookup.csv"

# Full table path
table_id = f"{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}"
table_id2 = f"{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME2}"
table_id3 = f"{PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME3}"

# Load credentials explicitly
credentials = service_account.Credentials.from_service_account_file(GCS_KEY_PATH)

# Initialize BigQuery client with credentials
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# Create dataset if not exists
dataset_ref = client.dataset(BQ_DATASET_NAME)

try:
    client.get_dataset(dataset_ref)
    print(f"✅ Dataset {BQ_DATASET_NAME} already exists.")
except:
    client.create_dataset(dataset_ref)
    print(f"🚀 Created dataset {BQ_DATASET_NAME}.")
    print(f"Dataset does not exist: {BQ_DATASET_NAME}")

# Check if table exists for yellow_taxi
try:
    client.get_table(table_id)
    print(f"✅ Table {BQ_TABLE_NAME} already exists.")
except NotFound:
    print(f"⚠️ Table {BQ_TABLE_NAME} does not exist. It will be created automatically when loading data.")

# Check if table exists for green_taxi
try:
    client.get_table(table_id2)
    print(f"✅ Table {BQ_TABLE_NAME2} already exists.")
except NotFound:
    print(f"⚠️ Table {BQ_TABLE_NAME2} does not exist. It will be created automatically when loading data.")

# Check if table exists for zone_lookup
try:
    client.get_table(table_id3)
    print(f"✅ Table {BQ_TABLE_NAME3} already exists.")
except NotFound:
    print(f"⚠️ Table {BQ_TABLE_NAME3} does not exist. It will be created automatically when loading data.")


# Function to check if table exists
def table_exists(client, table_id):
    try:
        client.get_table(table_id)
        print(f"✅ Table {table_id} already exists.")
        return True
    except NotFound:
        print(f"⚠️ Table {table_id} does not exist. It will be created automatically when loading data.")
        return False

# Define job configuration for loading Parquet data
parquet_job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,  # Specify Parquet format
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite existing data
)

# Define job configuration for loading CSV data
csv_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,  # Change to CSV
        skip_leading_rows=1,  # Skip header row
        autodetect=True,  # Let BigQuery infer schema
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

# Load data from GCS into BigQuery
if not table_exists(client, table_id):
    print(f"🚀 Loading data from {GCS_FILE_URI} into {table_id}...")
    load_job = client.load_table_from_uri(GCS_FILE_URI, table_id, job_config=parquet_job_config)
    load_job.result()  # Wait for the job to complete
else:
    print("✅ This table already exists!")

if not table_exists(client, table_id2):
    print(f"🚀 Loading data from {GCS_FILE_URI2} into {table_id2}...")
    load_job2 = client.load_table_from_uri(GCS_FILE_URI2, table_id2, job_config=parquet_job_config)
    load_job2.result()  # Wait for the job to complete
else:
    print("✅ This table already exists!")

if not table_exists(client, table_id3):
    print(f"🚀 Loading data from {GCS_FILE_URI3} into {table_id3}...")
    load_job3 = client.load_table_from_uri(GCS_FILE_URI3, table_id3, job_config=csv_job_config)
    load_job3.result()  # Wait for the job to complete
else:
    print("✅ This table already exists!")

# Confirm successful load
table = client.get_table(table_id) 
table2 = client.get_table(table_id2)
table3 = client.get_table(table_id3)
print(f"✅ Successfully loaded {table.num_rows} rows into {table_id}")
print(f"✅ Successfully loaded {table2.num_rows} rows into {table_id2}")
print(f"✅ Successfully loaded {table3.num_rows} rows into {table_id3}")
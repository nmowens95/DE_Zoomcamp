from google.cloud import storage
import os

# Set the GCS bucket and file paths
GCS_BUCKET_NAME = 'ny-rides-nate'  # GCS bucket name in terraform file
GCS_KEY_PATH = "/opt/airflow/keys/ny-rides-nate.json"

local_parquet_file_yellow = '/opt/airflow/csv_data/yellow_taxi_pickup_cleaned.parquet'  # Path to Parquet file
local_parquet_file_green = '/opt/airflow/csv_data/green_tripdata.parquet'
destination_blob_name_yellow = 'yellow_taxi_pickup_cleaned.parquet'  # GCS destination file name
destination_blob_name_green = 'green_tripdata.parquet'  # GCS destination file name

# Ensure the cleaned file exists
if not os.path.exists(local_parquet_file_yellow):
    raise FileNotFoundError(f"❌ Cleaned data file not found: {local_parquet_file_yellow}")
print(f"📂 Using cleaned data: {local_parquet_file_yellow}")

if not os.path.exists(local_parquet_file_green):
    raise FileNotFoundError(f"❌ Parquet file not found: {local_parquet_file_green}")
print(f"📂 Using green_tripdata file: {local_parquet_file_green}")

# Upload file to Google Cloud Storage
def upload_to_gcs(local_parquet_file_yellow, local_parquet_file_green, bucket_name, destination_blob_name_yellow, destination_blob_name_green):
    try:
        # Initialize a client
        storage_client = storage.Client.from_service_account_json(GCS_KEY_PATH)

        # Reference to your bucket
        # bucket = storage_client.get_bucket(bucket_name) deprecated
        bucket = storage_client.bucket(bucket_name)

        # Create a blob object for the destination file
        blob1 = bucket.blob(destination_blob_name_yellow)
        blob2 = bucket.blob(destination_blob_name_green)

        # Upload the Parquet files
        if blob1.exists():
            print(f"✅ {destination_blob_name_yellow} already exists in {bucket_name}. Skipping upload.")
        else:
            blob1.upload_from_filename(local_parquet_file_yellow)
            print(f"✅ {destination_blob_name_yellow} has now been uploaded to: {bucket_name}/{destination_blob_name_yellow}")

        if blob2.exists():
            print(f"✅ {destination_blob_name_green} already exists in {bucket_name}. Skipping upload.")
        else:
            blob2.upload_from_filename(local_parquet_file_green)
            print(f"File {local_parquet_file_green} has now been uploaded to: {bucket_name}/{destination_blob_name_green}.")
    except Exception as e:
        print(f"Ran into error: {e}")


# Run the upload
def main():
    try:
        upload_to_gcs(local_parquet_file_yellow, local_parquet_file_green, GCS_BUCKET_NAME, destination_blob_name_yellow, destination_blob_name_green)
        print("✅ Data successfully uploaded to GCS!")   
    except Exception as e:
        print(f"Ran into issues: {e}")

if __name__ == "__main__":
    main()
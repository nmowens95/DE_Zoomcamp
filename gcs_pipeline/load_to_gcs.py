from google.cloud import storage
import os

# Set the GCS bucket and file paths
GCS_BUCKET_NAME = 'ny-rides-nate'  # GCS bucket name in terraform file
GCS_KEY_PATH = "/opt/airflow/keys/ny-rides-nate.json"

# Local file access
local_parquet_file_yellow = '/opt/airflow/csv_data/yellow_taxi_pickup_cleaned.parquet'  # Path to Parquet file
local_parquet_file_green = '/opt/airflow/csv_data/green_tripdata.parquet'
local_zone_file = './csv_data/zone_pickup.csv'

# Blob storage
destination_blob_name_yellow = 'yellow_taxi_pickup_cleaned.parquet'  # GCS destination file name
destination_blob_name_green = 'green_tripdata.parquet'  # GCS destination file name
destination_blob_name_zone = 'zone_lookup.csv'  # GCS destination file name

# Ensure the cleaned file exists

# Yellow taxi
if not os.path.exists(local_parquet_file_yellow):
    raise FileNotFoundError(f"❌ Cleaned data file not found: {local_parquet_file_yellow}")
print(f"📂 Using cleaned data: {local_parquet_file_yellow}")

# Green pickup
if not os.path.exists(local_parquet_file_green):
    raise FileNotFoundError(f"❌ Parquet file not found: {local_parquet_file_green}")
print(f"📂 Using green_tripdata file: {local_parquet_file_green}")

# Zone lookup
if not os.path.exists(local_zone_file):
    raise FileNotFoundError(f"❌ CSV file not found: {local_zone_file}")
print(f"📂 Using zone_lookup file: {local_zone_file}")

# Upload file to Google Cloud Storage
def upload_to_gcs(local_parquet_file_yellow, local_parquet_file_green, local_zone_file, bucket_name, destination_blob_name_yellow, destination_blob_name_green, destination_blob_name_zone):
    try:
        # Initialize a client
        storage_client = storage.Client.from_service_account_json(GCS_KEY_PATH)

        # Reference to your bucket
        # bucket = storage_client.get_bucket(bucket_name) deprecated
        bucket = storage_client.bucket(bucket_name)

        # Create a blob object for the destination file
        blob1 = bucket.blob(destination_blob_name_yellow)
        blob2 = bucket.blob(destination_blob_name_green)
        blob3 = bucket.blob(destination_blob_name_zone)

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

        if blob3.exists():
            print(f"✅ {destination_blob_name_zone} already exists in {bucket_name}. Skipping upload.")
        else:
            blob3.upload_from_filename(local_zone_file)
            print(f"File {local_zone_file} has now been uploaded to: {bucket_name}/{destination_blob_name_zone}.")
    
    except Exception as e:
        print(f"Ran into error: {e}")


# Run the upload
def main():
    try:
        upload_to_gcs(local_parquet_file_yellow, local_parquet_file_green, local_zone_file, GCS_BUCKET_NAME, destination_blob_name_yellow, destination_blob_name_green, destination_blob_name_zone)
        print("✅ Data successfully uploaded to GCS!")   
    except Exception as e:
        print(f"Ran into issues: {e}")

if __name__ == "__main__":
    main()
import os
import sys
import urllib.request
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden

#config for data source
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024

def initialize_gcs():
    """Validates env vars and gcs bucket access"""
    #tries to load env vars from .env file in same dir as the script
    load_dotenv(dotenv_path=Path(__file__).resolve().parent / '.env')
    
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if not bucket_name or not creds:
        print(f"ERROR: missing env vars. Ensure GCS_BUCKET_NAME and GOOGLE_APPLICATION_CREDENTIALS are set in .env")
        sys.exit(1)

    try:
        client = storage.Client()
        # checks if credentials are valid by fetching project info
        print(f"authenticated: Project {client.project}")
        
        try:
            bucket_obj = client.get_bucket(bucket_name)
            print(f"bucket '{bucket_name}' verified.")
        except NotFound:
            print(f"bucket '{bucket_name}' not found. Creating...")
            bucket_obj = client.create_bucket(bucket_name)
            print(f"created bucket '{bucket_name}'.")
            
        return client, bucket_obj

    except Forbidden:
        print(f"ERROR: Permission denied for bucket '{bucket_name}'.")
        sys.exit(1)
    
    #catch all for other GCS init errors
    except Exception as e:
        print(f"ERROR: GCS Initialization failed: {e}")
        sys.exit(1)

def download_file(month):
    file_name = f"yellow_tripdata_2024-{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    url = f"{BASE_URL}{month}.parquet"

    #skip downloads if file exists locally
    if os.path.exists(file_path):
        print(f"Skipping download: {file_name} already exists locally")
        return file_path

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        return file_path
    except Exception as e:
        print(f"failed to download {month}: {e}")
        return None

def upload_to_gcs(file_path, client, bucket):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    #skip if file already exists in GCS
    if blob.exists(client):
        print(f"skipping upload: {blob_name} already exists in GCS")
        #rmv local file after confirming it exists
        if os.path.exists(file_path):
            os.remove(file_path)
        return

    for attempt in range(3):
        try:
            print(f"Uploading {blob_name} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            
            if blob.exists(client):
                print(f"verified & uploaded: {blob_name}")
                os.remove(file_path) 
                return
        except Exception as e:
            print(f"retry {attempt + 1} for {blob_name}: {e}")
        time.sleep(2)
    print(f"failed to upload {blob_name} after 3 attempts")

if __name__ == "__main__":
    # init GCS client and bucket
    gcs_client, target_bucket = initialize_gcs()
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    #concurrent downloads
    print("\n downloads ")
    with ThreadPoolExecutor(max_workers=4) as executor:
        paths = list(executor.map(download_file, MONTHS))

    #concurrent uploads
    print("\n uploads ")
    valid_paths = [p for p in paths if p]
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Pass the client and bucket objects to the lambda
        executor.map(lambda p: upload_to_gcs(p, gcs_client, target_bucket), valid_paths)

    print("\nWorkflow complete.")
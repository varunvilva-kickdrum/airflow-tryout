import requests
import boto3, time

# Constants
DATA_URL = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
LOCAL_FILE = "/tmp/fatal_collisions.csv"
S3_KEY = "datasets/fatal_collisions.csv"

def download_data():
    print(f"Downloading data from {DATA_URL}")
    response = requests.get(DATA_URL)
    response.raise_for_status()  # Ensure we fail on error
    with open(LOCAL_FILE, "wb") as f:
        f.write(response.content)
    print(f"File saved to {LOCAL_FILE}")

def upload_to_s3(s3_bucket, local_file, s3_key):
    print(f"Uploading {local_file} to s3://{s3_bucket}/{s3_key}")
    s3 = boto3.client("s3")
    s3.upload_file(local_file, s3_bucket, s3_key)
    print("Upload complete.")

def download_upload_task(s3_bucket=None):
    time.sleep(5)
    download_data()
    time.sleep(5) 
    upload_to_s3(s3_bucket, LOCAL_FILE, S3_KEY)

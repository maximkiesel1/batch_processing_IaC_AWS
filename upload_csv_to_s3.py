import os
import re
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import json

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Load the kms key arn and other configurations from the configuration file
with open(os.path.join(script_dir, 'config.json'), 'r') as f:
    config = json.load(f)

kms_key_arn = config.get('KMS_KEY_ARN')
s3_bucket_name = config.get('S3_BUCKET_NAME', 'data-ingestion-bucket-kiesel')  # Default value if not in config

s3_client = boto3.client('s3', region_name="eu-north-1")

def data_ingestion(folder_path, s3_bucket_name, kms_key_id):
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    latest_date = None
    latest_csv_file = None

    for csv_file in csv_files:
        date_match = re.match(r'^(\d{8})', csv_file)
        if date_match:
            date_str = date_match.group(1)
            try:
                file_date = datetime.strptime(date_str, '%Y%m%d')
                if not latest_date or file_date > latest_date:
                    latest_date = file_date
                    latest_csv_file = csv_file
            except ValueError:
                print(f"Invalid date format in CSV file {csv_file}")

    if latest_csv_file:
        local_csv_path = os.path.join(folder_path, latest_csv_file)
        try:
            s3_client.upload_file(local_csv_path, s3_bucket_name, latest_csv_file, ExtraArgs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': kms_key_id})
            print(f"Latest CSV file {local_csv_path} uploaded to S3 bucket {s3_bucket_name}")
        except ClientError as e:
            print(f"Error uploading latest CSV file {local_csv_path} to S3 bucket {s3_bucket_name}: {e}")
    else:
        print("No valid CSV files found in the folder")

# Use a relative path for the data directory
data_dir = os.path.join(script_dir, 'data')

# Upload the latest local CSV file to the input S3 bucket
data_ingestion(data_dir, s3_bucket_name, kms_key_arn)

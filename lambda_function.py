import os
import re
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')

# Function to upload the latest CSV file from a folder to an S3 bucket
def upload_latest_csv_to_s3(folder_path, s3_bucket_name):
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
            s3_client.upload_file(local_csv_path, s3_bucket_name, latest_csv_file)
            print(f"Latest CSV file {local_csv_path} uploaded to S3 bucket {s3_bucket_name}")
        except ClientError as e:
            print(f"Error uploading latest CSV file {local_csv_path} to S3 bucket {s3_bucket_name}: {e}")
    else:
        print("No valid CSV files found in the folder")

# Upload the latest local CSV file to the input S3 bucket
upload_latest_csv_to_s3('path/to/your/local/csv/folder', 'mein-lokaler-csv-bucket')

def lambda_handler(event, context):
    folder_path = os.environ['FOLDER_PATH']
    s3_bucket_name = os.environ['S3_BUCKET_NAME']
    upload_latest_csv_to_s3(folder_path, s3_bucket_name)
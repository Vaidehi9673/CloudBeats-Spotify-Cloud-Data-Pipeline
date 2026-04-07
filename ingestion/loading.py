import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError

#load key from the .env file into memory
load_dotenv()

# 1. Plug in your details
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
SECRET_KEY = os.getenv('AWS_SECRET_KEY')
BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')

# 2. source and destination file name
local_file = "raw_data/spotify_data.csv"  
s3_destination_path = "landing/spotify_data.csv"

# 3. Connect to S3

def upload_to_s3(file_name, bucket, s3_path):
    s3_client = boto3.client(
        's3',
        aws_access_key_id = ACCESS_KEY,
        aws_secret_access_key = SECRET_KEY
    )

    try:
        print(f"Starting upload: {file_name} to S3://{bucket}/{s3_path}...")
        #perform upload
        s3_client.upload_file(file_name, bucket, s3_path)
        print("Upload Successful! Data is now in the Cloud Landing Zone")

    except FileNotFoundError:
        print("Error: The local csv file was not found. Check your file path")
    except NoCredentialsError:
        print("Error: AWS credentials not found. check your .env file")
    except Exception as e:
        print(f"An unexpected error occured: {e}")

if __name__ == "__main__":
    upload_to_s3(local_file, BUCKET_NAME, s3_destination_path)
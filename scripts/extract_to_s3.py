import boto3
import os
import pandas as pd
from datetime import datetime

# 1. Configuration - CHANGE THIS to the bucket name you used in Terraform
BUCKET_NAME = "olist-raw-zone-arjun-cs-032026" 
LOCAL_DATA_PATH = "data/raw_olist/"

def upload_to_s3():
    # Initialize the S3 client (it uses the keys from your 'aws configure')
    s3_client = boto3.client('s3')
    
    # Get today's date to create a 'partition'
    execution_date = datetime.now().strftime("%Y-%m-%d")

    # Loop through the files in your local data folder
    for filename in os.listdir(LOCAL_DATA_PATH):
        if filename.endswith(".csv"):
            file_path = os.path.join(LOCAL_DATA_PATH, filename)
            
            print(f"Reading {filename}...")
            
            # Read CSV and convert to JSON string 
            # (We do this to simul ate receiving data from a modern Web API)
            df = pd.read_csv(file_path)
            json_data = df.to_json(orient='records')
            
            # Create a clean folder name (e.g., 'olist_orders_dataset.csv' -> 'orders')
            table_name = filename.replace("_dataset.csv", "").replace("olist_", "")
            
            # Define the 'Key' (the path inside S3)
            # Structure: raw/table_name/date/file.json
            s3_key = f"raw/{table_name}/{execution_date}/{table_name}.json"
            
            print(f"Uploading to s3://{BUCKET_NAME}/{s3_key}...")
            
            # Upload the JSON string directly to S3
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=json_data
            )

if __name__ == "__main__":
    upload_to_s3()
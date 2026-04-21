import boto3
import os

s3 = boto3.client("s3", region_name="ap-south-1")
bucket_name = "walmart-dbt-project"
local_folder = r"C:\Users\BANSARI MODI\Documents\e2e_projects\Walmart\data"
s3_folder = "data"

def upload_file(file_path, bucket, s3_folder):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    file_name = os.path.basename(file_path)
    s3_key = f"{s3_folder}/{file_name}"

    try:
        s3.upload_file(file_path, bucket, s3_key)
        print(f"Uploaded: {file_path} -> s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"Error uploading {file_path}: {e}")

def main():
    if not os.path.exists(local_folder):
        print(f"Folder not found: {local_folder}")
        return

    for file_name in os.listdir(local_folder):
        if file_name.endswith(".csv"):
            file_path = os.path.join(local_folder, file_name)
            upload_file(file_path, bucket_name, s3_folder)

if __name__ == "__main__":
    main()
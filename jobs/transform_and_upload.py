import argparse
import os
import glob
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from minio import Minio
from minio.error import S3Error

# Ensure local directories exist
os.makedirs("/tmp/processed_data/temp_csv", exist_ok=True)
os.makedirs("/tmp/processed_data/temp_parquet", exist_ok=True)
os.makedirs("/tmp/raw_data", exist_ok=True)

def upload_sample_data(client, raw_bucket):
    """Upload a sample CSV if raw bucket is empty."""
    sample_file = "/tmp/raw_data/sample.csv"

    # Create sample dataset using pandas
    data = {
        "age": [25, 32, 45, 29, 52],
        "sex": ["male", "female", "male", "female", "male"],
        "bmi": [22.4, 28.9, 31.2, 24.7, 29.4],
        "charges": [3200.5, 4300.1, 8200.0, 2900.3, 9100.2]
    }
    df = pd.DataFrame(data)
    df.to_csv(sample_file, index=False)

    # Upload to MinIO
    client.fput_object(raw_bucket, "sample.csv", sample_file)
    print(f"Uploaded sample.csv to bucket {raw_bucket}")

def transform_and_upload(endpoint, access_key, secret_key, raw_bucket, processed_bucket):
    print("Initializing Spark session...")
    spark = SparkSession.builder.appName("ETL Medical Cost").getOrCreate()
    print("Spark session started.")

    print("Initializing MinIO client...")
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    print(f"Connected to MinIO at {endpoint}")

    # Ensure raw and processed buckets exist
    for bucket in [raw_bucket, processed_bucket]:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"Created bucket: {bucket}")
            else:
                print(f"Bucket exists: {bucket}")
        except S3Error as e:
            print(f"Error ensuring bucket exists: {e}")

    # List objects in raw bucket
    try:
        objects = client.list_objects(raw_bucket)
        raw_files = [obj.object_name for obj in objects]
        if not raw_files:
            print(f"No files found in bucket {raw_bucket}, uploading sample data...")
            upload_sample_data(client, raw_bucket)
            raw_files = ["sample.csv"]
        else:
            print(f"Found {len(raw_files)} file(s) in bucket {raw_bucket}: {raw_files}")
    except S3Error as e:
        print(f"Error listing objects: {e}")
        return

    # Download raw files locally
    for file_name in raw_files:
        local_path = f"/tmp/raw_data/{file_name}"
        client.fget_object(raw_bucket, file_name, local_path)
        print(f"Downloaded {file_name} to {local_path}")

    # Read CSV files with PySpark
    df = spark.read.csv("/tmp/raw_data/*.csv", header=True, inferSchema=True)
    print(f"Read {df.count()} rows from raw CSV files.")

    # Example transformation: filter patients with age > 30 and select columns
    transformed_df = df.filter(col("age") > 30).select("age", "sex", "bmi", "charges")
    print(f"Transformed data has {transformed_df.count()} rows after filtering.")

    # ---- Save as CSV ----
    csv_dir = "/tmp/processed_data/temp_csv"
    transformed_df.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_dir)
    csv_part = glob.glob(f"{csv_dir}/part-*.csv")[0]
    csv_output = "/tmp/processed_data/transformed.csv"
    os.rename(csv_part, csv_output)
    client.fput_object(processed_bucket, "transformed.csv", csv_output)
    print(f"Uploaded transformed.csv to bucket {processed_bucket}")

    # ---- Save as Parquet ----
    parquet_dir = "/tmp/processed_data/temp_parquet"
    transformed_df.coalesce(1).write.mode("overwrite").parquet(parquet_dir)
    parquet_part = glob.glob(f"{parquet_dir}/part-*.parquet")[0]
    parquet_output = "/tmp/processed_data/transformed.parquet"
    os.rename(parquet_part, parquet_output)
    client.fput_object(processed_bucket, "transformed.parquet", parquet_output)
    print(f"Uploaded transformed.parquet to bucket {processed_bucket}")

    spark.stop()
    print("Spark session stopped. ETL process completed successfully.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--access_key", required=True)
    parser.add_argument("--secret_key", required=True)
    parser.add_argument("--raw_bucket", required=True)
    parser.add_argument("--processed_bucket", required=True)

    args = parser.parse_args()

    transform_and_upload(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        raw_bucket=args.raw_bucket,
        processed_bucket=args.processed_bucket
    )

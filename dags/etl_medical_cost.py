from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
from minio import Minio


# Paths inside container
DATA_DIR = "/opt/airflow/data"
UPLOAD_SCRIPT = "/opt/airflow/jobs/upload_to_minio.py"
TRANSFORM_SCRIPT = "/opt/airflow/jobs/transform_and_upload.py"


def download_dataset():
    subprocess.run([
        "kaggle", "datasets", "download",
        "-d", "mirichoi0218/insurance",
        "-p", DATA_DIR, "--unzip"
    ], check=True)


def upload_raw():
    subprocess.run([
        "python", UPLOAD_SCRIPT,
        "--endpoint", "minio:9000",
        "--access_key", "minio",
        "--secret_key", "minio123",
        "--bucket", "raw-data",
        "--path", DATA_DIR
    ], check=True)


def transform_data():
    subprocess.run([
        "python", TRANSFORM_SCRIPT,
        "--endpoint", "minio:9000",
        "--access_key", "minio",
        "--secret_key", "minio123",
        "--raw_bucket", "raw-data",
        "--processed_bucket", "processed-data"
    ], check=True)


def list_bucket_contents(bucket_name):
    """Print contents of a MinIO bucket"""
    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False,
    )
    print(f"\n📂 Listing objects in bucket: {bucket_name}")
    for obj in client.list_objects(bucket_name, recursive=True):
        print(f" - {obj.object_name} ({obj.size} bytes)")


with DAG(
    "etl_medical_cost",
    default_args={"owner": "airflow"},
    description="ETL for Medical Cost dataset using PySpark + MinIO",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="download_dataset",
        python_callable=download_dataset,
    )

    t2 = PythonOperator(
        task_id="upload_raw_to_minio",
        python_callable=upload_raw,
    )

    t3 = PythonOperator(
        task_id="list_raw_bucket",
        python_callable=list_bucket_contents,
        op_args=["raw-data"],
    )

    t4 = PythonOperator(
        task_id="transform_with_pyspark",
        python_callable=transform_data,
    )

    t5 = PythonOperator(
        task_id="list_processed_bucket",
        python_callable=list_bucket_contents,
        op_args=["processed-data"],
    )

    # DAG workflow
    t1 >> t2 >> t3 >> t4 >> t5

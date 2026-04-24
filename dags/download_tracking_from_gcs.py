import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage


def download_tracking_csv_from_gcs():
    bucket_name = os.environ["GCP_BUCKET_NAME"]
    object_name = os.environ.get("GCP_OBJECT_NAME", "tracking.csv")
    destination_path = os.environ.get(
        "LOCAL_TRACKING_PATH",
        "/opt/airflow/data/raw/tracking.csv"
    )
    credentials_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

    os.makedirs(os.path.dirname(destination_path), exist_ok=True)

    client = storage.Client.from_service_account_json(credentials_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    if not blob.exists():
        raise FileNotFoundError(f"Object not found: gs://{bucket_name}/{object_name}")

    blob.download_to_filename(destination_path)

    file_size = os.path.getsize(destination_path)

    print("Downloaded file successfully")
    print(f"Source      : gs://{bucket_name}/{object_name}")
    print(f"Destination : {destination_path}")
    print(f"File size   : {file_size} bytes")


with DAG(
    dag_id="download_tracking_from_gcs",
    description="Download tracking.csv from GCP Cloud Storage raw data lake",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["gcp", "cloud-storage", "data-lake", "recruitment-pipeline"],
) as dag:

    download_tracking_csv = PythonOperator(
        task_id="download_tracking_csv",
        python_callable=download_tracking_csv_from_gcs,
    )

    download_tracking_csv
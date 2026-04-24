from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="replay_tracking_csv_to_kafka",
    description="Replay tracking.csv into Kafka topic tracking-events",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["kafka", "replay", "near-real-time", "recruitment-pipeline"],
) as dag:

    replay_csv_to_kafka = BashOperator(
        task_id="replay_csv_to_kafka",
        bash_command="""
        python /opt/airflow/jobs/replay_tracking_csv_to_kafka.py \
          --csv-path /opt/airflow/data/raw/tracking.csv \
          --bootstrap-server rr-kafka:29092 \
          --topic tracking-events \
          --sleep 0.001
        """,
    )

    replay_csv_to_kafka
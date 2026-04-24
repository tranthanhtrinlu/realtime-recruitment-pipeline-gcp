from datetime import datetime

import mysql.connector
from cassandra.cluster import Cluster

from airflow import DAG
from airflow.operators.python import PythonOperator


MYSQL_CONFIG = {
    "host": "rr-mysql",
    "port": 3306,
    "database": "etl_dw",
    "user": "etl_user",
    "password": "etl_password",
}


def check_cassandra_raw_events():
    cluster = Cluster(["rr-cassandra"], port=9042)
    session = cluster.connect()

    try:
        row = session.execute("SELECT COUNT(*) AS total_rows FROM logs.tracking_raw").one()
        total_rows = row.total_rows

        print(f"Cassandra raw rows: {total_rows}")

        if total_rows <= 0:
            raise ValueError("Cassandra quality check failed: logs.tracking_raw is empty")

        return total_rows

    finally:
        session.shutdown()
        cluster.shutdown()


def write_quality_log(check_name, status, message):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_quality_log (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                check_name VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL,
                message TEXT,
                checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            INSERT INTO pipeline_quality_log (check_name, status, message)
            VALUES (%s, %s, %s)
        """, (check_name, status, message))

        conn.commit()

    finally:
        cursor.close()
        conn.close()


def check_mysql_hourly_fact():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute("""
            SELECT
                COUNT(*) AS fact_rows,
                COALESCE(SUM(total_events), 0) AS total_events,
                COALESCE(ROUND(SUM(total_bid), 2), 0) AS total_bid
            FROM fact_events_hourly
        """)

        result = cursor.fetchone()

        fact_rows = int(result["fact_rows"])
        total_events = int(result["total_events"])
        total_bid = float(result["total_bid"])

        print(f"MySQL fact rows    : {fact_rows}")
        print(f"MySQL total events : {total_events}")
        print(f"MySQL total bid    : {total_bid}")

        if fact_rows <= 0:
            raise ValueError("MySQL quality check failed: fact_events_hourly has no rows")

        if total_events <= 0:
            raise ValueError("MySQL quality check failed: total_events is zero")

        message = (
            f"fact_rows={fact_rows}, "
            f"total_events={total_events}, "
            f"total_bid={total_bid}"
        )

        write_quality_log(
            check_name="mysql_fact_events_hourly_check",
            status="success",
            message=message,
        )

        return message

    except Exception as error:
        write_quality_log(
            check_name="mysql_fact_events_hourly_check",
            status="failed",
            message=str(error),
        )
        raise

    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="quality_check_cassandra_mysql",
    description="Quality checks for Cassandra raw table and MySQL hourly fact table",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["quality-check", "cassandra", "mysql", "recruitment-pipeline"],
) as dag:

    check_cassandra = PythonOperator(
        task_id="check_cassandra_raw_events",
        python_callable=check_cassandra_raw_events,
    )

    check_mysql = PythonOperator(
        task_id="check_mysql_hourly_fact",
        python_callable=check_mysql_hourly_fact,
    )

    check_cassandra >> check_mysql
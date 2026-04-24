\# End-to-End Near Real-Time Recruitment Data Pipeline with Kafka, Spark, Airflow, Docker, and GCP



\## Overview



This project is an end-to-end near real-time data engineering pipeline for recruitment tracking events.



The pipeline ingests raw event data from `tracking.csv`, stores the source file in Google Cloud Storage as a raw data lake, replays events into Kafka, processes them with Spark Structured Streaming, stores raw events in Cassandra, writes hourly aggregated facts into MySQL, validates data quality with Airflow, and visualizes business metrics in Grafana.



This project is designed as a junior/middle-level Data Engineer portfolio project.



\---



\## Architecture



```text

GCP Cloud Storage

&#x20;     |

&#x20;     | Airflow DAG: download\_tracking\_from\_gcs

&#x20;     v

tracking.csv

&#x20;     |

&#x20;     | Airflow DAG / Python replay script

&#x20;     v

Kafka topic: tracking-events

&#x20;     |

&#x20;     | Spark Structured Streaming

&#x20;     v

+----------------------+----------------------+

| Cassandra             | MySQL                |

| Raw event storage     | Hourly fact table    |

| logs.tracking\_raw     | fact\_events\_hourly   |

+----------------------+----------------------+

&#x20;                             |

&#x20;                             v

&#x20;                         Grafana Dashboard



Airflow also runs data quality checks against Cassandra and MySQL.



Tech Stack

Google Cloud Storage

Docker

Apache Kafka

Apache Spark Structured Streaming

Apache Cassandra

MySQL

Apache Airflow

Grafana

Python



Key Features

Stores raw source data in GCP Cloud Storage.

Replays CSV records into Kafka to simulate near real-time event streaming.

Processes Kafka events with Spark Structured Streaming.

Stores raw events in Cassandra as a raw data layer.

Aggregates hourly event metrics into MySQL.

Uses Airflow to orchestrate GCS download, Kafka replay, and data quality checks.

Uses Grafana to visualize recruitment event metrics.

Includes quality check logging in MySQL.





Data Flow

tracking.csv

&#x20; -> GCP Cloud Storage

&#x20; -> Airflow download DAG

&#x20; -> Kafka topic tracking-events

&#x20; -> Spark Structured Streaming

&#x20; -> Cassandra raw table

&#x20; -> MySQL hourly fact table

&#x20; -> Grafana dashboard

&#x20; -> Airflow quality checks





realtime-recruitment-pipeline-gcp/

├── dags/

│   ├── download\_tracking\_from\_gcs.py

│   ├── replay\_tracking\_csv\_to\_kafka.py

│   └── quality\_check\_cassandra\_mysql.py

├── data/

│   ├── raw/

│   │   └── tracking.csv

│   └── sample/

│       └── tracking\_sample.csv

├── docs/

│   ├── gcp-bucket-tracking.png

│   ├── docker-containers.png

│   ├── spark-streaming-ui.png

│   ├── grafana-dashboard.png

│   ├── airflow-dags.png

│   ├── airflow-download-gcs-success.png

│   ├── airflow-quality-check-success.png

│   └── mysql-quality-log.png

├── jobs/

│   ├── replay\_tracking\_csv\_to\_kafka.py

│   └── kafka\_to\_cassandra\_mysql\_stream.py

├── sql/

│   ├── init\_mysql.sql

│   └── init\_cassandra.cql

├── docker-compose.yml

├── requirements.txt

├── .env.example

├── .gitignore

└── README.md



GCP Cloud Storage Raw Data Lake

The source file tracking.csv is uploaded to a GCP Cloud Storage bucket.



Docker Infrastructure

The project runs the main data platform locally using Docker Compose.

Services:



Kafka

Zookeeper

Spark Master

Spark Worker

Cassandra

MySQL

Grafana

Airflow Webserver

Airflow Scheduler



Spark Structured Streaming

Spark reads events from Kafka and writes:

Raw events to Cassandra

Hourly aggregated metrics to MySQL



Grafana Dashboard

Grafana connects to MySQL and visualizes recruitment event metrics.

Dashboard metrics include:

Total Events

Total Bid

Events Over Time

Event Type Trend Over Time

Events by Event Type



Airflow Orchestration

Airflow manages orchestration tasks:

Download tracking.csv from GCP Cloud Storage

Replay CSV data into Kafka

Run quality checks on Cassandra and MySQL



GCS Download DAG

Data Quality Check DAG



Data Quality Result

The quality check validates that data exists in Cassandra and MySQL, then writes the result to pipeline\_quality\_log.



MySQL Fact Table

Main fact table:

fact\_events\_hourly



Example columns:

event\_date

event\_hour

custom\_track

total\_events

total\_bid

updated\_at



Example quality check result:

fact\_rows=8877

total\_events=100000

total\_bid=24132.0



How to Run

1\. Start Docker services

docker compose up -d



2\. Create Kafka topic

docker exec -it rr-kafka kafka-topics \\

&#x20; --bootstrap-server rr-kafka:29092 \\

&#x20; --create \\

&#x20; --topic tracking-events \\

&#x20; --partitions 1 \\

&#x20; --replication-factor 1



3\. Initialize Cassandra schema

docker cp sql/init\_cassandra.cql rr-cassandra:/init\_cassandra.cql

docker exec -it rr-cassandra cqlsh -f /init\_cassandra.cql



4\. Run Spark Streaming job

docker exec -it rr-spark-master spark-submit \\

&#x20; --master spark://rr-spark-master:7077 \\

&#x20; --packages org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector\_2.12:3.5.1,com.mysql:mysql-connector-j:8.0.33 \\

&#x20; --conf spark.cassandra.connection.host=rr-cassandra \\

&#x20; --conf spark.cassandra.connection.port=9042 \\

&#x20; /opt/project/jobs/kafka\_to\_cassandra\_mysql\_stream.py



5\. Replay CSV into Kafka

python jobs/replay\_tracking\_csv\_to\_kafka.py --sleep 0.001



6\. Open UIs

Spark UI   : http://localhost:8080

Grafana    : http://localhost:3000

Airflow    : http://localhost:8090



Default Grafana login:

username: admin

password: admin



Default Airflow login:

username: admin

password: admin



Important Notes

The full raw dataset is not pushed to GitHub.

A sample dataset is available in data/sample/tracking\_sample.csv.

GCP service account keys are excluded using .gitignore.

This project uses local Docker services to avoid unnecessary GCP costs.

GCP is used mainly for Cloud Storage as a raw data lake.



Portfolio Highlights

This project demonstrates:

Batch-to-stream simulation using Kafka

Near real-time stream processing with Spark Structured Streaming

Raw event storage with Cassandra

Data warehouse-style hourly aggregation with MySQL

Workflow orchestration with Airflow

Data quality checks

Dashboarding with Grafana

GCP Cloud Storage integration

Dockerized data platform setup



Author

Tran Thanh Tri

Data Engineer Portfolio Project


import mysql.connector

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
    hour,
    coalesce,
    current_timestamp,
    lit,
    date_format,
    count,
    sum as spark_sum,
    sha2,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


KAFKA_BOOTSTRAP_SERVERS = "rr-kafka:29092"
KAFKA_TOPIC = "tracking-events"

MYSQL_HOST = "rr-mysql"
MYSQL_PORT = 3306
MYSQL_DATABASE = "etl_dw"
MYSQL_USER = "etl_user"
MYSQL_PASSWORD = "etl_password"

CHECKPOINT_PATH = "/opt/project/checkpoints/kafka_to_cassandra_mysql"


def get_event_schema():
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_date", StringType(), True),
        StructField("event_hour", IntegerType(), True),
        StructField("custom_track", StringType(), True),
        StructField("uid", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("campaign_id", StringType(), True),
        StructField("group_id", StringType(), True),
        StructField("publisher_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("bid", DoubleType(), True),
        StructField("source_file", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("replayed_at", StringType(), True),
    ])


def create_mysql_batch_log_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stream_batch_log (
            batch_id BIGINT PRIMARY KEY,
            status VARCHAR(50) NOT NULL,
            row_count BIGINT NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)


def is_batch_processed(cursor, batch_id):
    cursor.execute(
        "SELECT batch_id FROM stream_batch_log WHERE batch_id = %s",
        (int(batch_id),)
    )
    return cursor.fetchone() is not None


def write_batch_to_sinks(batch_df, batch_id):
    batch_df.persist()

    conn = None
    cursor = None

    try:
        row_count = batch_df.count()

        if row_count == 0:
            print(f"[BATCH {batch_id}] Empty batch. Skip.")
            return

        print("=" * 80)
        print(f"[BATCH {batch_id}] Start processing {row_count} rows")
        print("=" * 80)

        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DATABASE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
        )
        cursor = conn.cursor()

        create_mysql_batch_log_table(cursor)

        if is_batch_processed(cursor, batch_id):
            print(f"[BATCH {batch_id}] Already processed. Skip.")
            return

        raw_df = batch_df.select(
            "event_id",
            "event_time",
            "event_date",
            "event_hour",
            "custom_track",
            "uid",
            "job_id",
            "campaign_id",
            "group_id",
            "publisher_id",
            "device_type",
            "revenue",
            "bid",
            "raw_json",
            "ingested_at",
        )

        raw_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(keyspace="logs", table="tracking_raw") \
            .save()

        print(f"[BATCH {batch_id}] Written raw events to Cassandra")

        fact_df = batch_df.groupBy(
            "event_date",
            "event_hour",
            "custom_track"
        ).agg(
            count("*").alias("total_events"),
            spark_sum(coalesce(col("bid"), lit(0.0))).alias("total_bid")
        )

        fact_rows = fact_df.collect()

        upsert_sql = """
            INSERT INTO fact_events_hourly (
                event_date,
                event_hour,
                custom_track,
                total_events,
                total_bid
            )
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_events = total_events + VALUES(total_events),
                total_bid = total_bid + VALUES(total_bid),
                updated_at = CURRENT_TIMESTAMP
        """

        values = []

        for row in fact_rows:
            values.append((
                row["event_date"],
                int(row["event_hour"]),
                row["custom_track"],
                int(row["total_events"]),
                float(row["total_bid"] or 0.0),
            ))

        if values:
            cursor.executemany(upsert_sql, values)

        cursor.execute(
            """
            INSERT INTO stream_batch_log (batch_id, status, row_count)
            VALUES (%s, %s, %s)
            """,
            (int(batch_id), "success", int(row_count))
        )

        conn.commit()

        print(f"[BATCH {batch_id}] Written hourly facts to MySQL")
        print(f"[BATCH {batch_id}] DONE")

    except Exception as error:
        print(f"[BATCH {batch_id}] ERROR: {error}")
        if conn:
            conn.rollback()
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        batch_df.unpersist()


def main():
    spark = SparkSession.builder \
        .appName("KafkaToCassandraMySQLStreaming") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.cassandra.connection.host", "rr-cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = get_event_schema()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    json_df = kafka_df.selectExpr(
        "CAST(value AS STRING) AS raw_json",
        "timestamp AS kafka_timestamp"
    )

    parsed_df = json_df.select(
        from_json(col("raw_json"), schema).alias("event"),
        col("raw_json"),
        col("kafka_timestamp"),
    )

    event_time_col = coalesce(
        to_timestamp(col("event.event_time")),
        col("kafka_timestamp")
    )

    event_date_col = date_format(
        coalesce(
            to_date(col("event.event_date")),
            to_date(event_time_col)
        ),
        "yyyy-MM-dd"
    )

    clean_df = parsed_df.select(
        coalesce(col("event.event_id"), sha2(col("raw_json"), 256)).alias("event_id"),
        event_time_col.alias("event_time"),
        event_date_col.alias("event_date"),
        coalesce(col("event.event_hour"), hour(event_time_col)).cast("int").alias("event_hour"),
        coalesce(col("event.custom_track"), lit("unknown")).alias("custom_track"),
        col("event.uid").alias("uid"),
        col("event.job_id").alias("job_id"),
        col("event.campaign_id").alias("campaign_id"),
        col("event.group_id").alias("group_id"),
        col("event.publisher_id").alias("publisher_id"),
        col("event.device_type").alias("device_type"),
        col("event.revenue").cast("double").alias("revenue"),
        col("event.bid").cast("double").alias("bid"),
        col("raw_json"),
        current_timestamp().alias("ingested_at"),
    ).filter(
        col("event_date").isNotNull()
        & col("event_hour").isNotNull()
        & col("custom_track").isNotNull()
    )

    query = clean_df.writeStream \
        .foreachBatch(write_batch_to_sinks) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("=" * 80)
    print("Spark Structured Streaming started")
    print(f"Kafka topic      : {KAFKA_TOPIC}")
    print(f"Checkpoint path  : {CHECKPOINT_PATH}")
    print("=" * 80)

    query.awaitTermination()


if __name__ == "__main__":
    main()
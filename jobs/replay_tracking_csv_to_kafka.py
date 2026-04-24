import argparse
import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer


def clean_value(value):
    if value is None:
        return None

    value = str(value).strip()

    if value == "":
        return None

    if value.lower() in ["null", "none", "nan", "na"]:
        return None

    return value


def to_float(value):
    value = clean_value(value)
    if value is None:
        return None

    try:
        return float(value)
    except ValueError:
        return None


def to_int(value):
    value = clean_value(value)
    if value is None:
        return None

    try:
        return int(float(value))
    except ValueError:
        return None


def build_event(row, row_number):
    event = {}

    for key, value in row.items():
        if key is None:
            continue

        clean_key = key.strip()
        event[clean_key] = clean_value(value)

    event_id = (
        event.get("event_id")
        or event.get("id")
        or f"csv-row-{row_number}"
    )

    event_time = (
        event.get("event_time")
        or event.get("create_time")
        or event.get("ts")
    )

    event_date = event.get("event_date")
    event_hour = to_int(event.get("event_hour"))

    event["event_id"] = event_id
    event["event_time"] = event_time
    event["event_date"] = event_date
    event["event_hour"] = event_hour
    event["custom_track"] = event.get("custom_track")
    event["uid"] = event.get("uid")
    event["job_id"] = event.get("job_id")
    event["campaign_id"] = event.get("campaign_id")
    event["group_id"] = event.get("group_id")
    event["publisher_id"] = event.get("publisher_id")
    event["device_type"] = event.get("device_type")
    event["revenue"] = to_float(event.get("revenue"))
    event["bid"] = to_float(event.get("bid"))

    event["source_file"] = "tracking.csv"
    event["source_system"] = "gcp_cloud_storage_raw_lake"
    event["replayed_at"] = datetime.now(timezone.utc).isoformat()

    return event


def main():
    parser = argparse.ArgumentParser(
        description="Replay tracking.csv rows into Kafka topic as JSON events."
    )

    parser.add_argument(
        "--csv-path",
        default="data/raw/tracking.csv",
        help="Path to tracking.csv"
    )

    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server"
    )

    parser.add_argument(
        "--topic",
        default="tracking-events",
        help="Kafka topic name"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of rows to send. 0 means send all rows."
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0.01,
        help="Sleep seconds between messages to simulate near real-time streaming."
    )

    args = parser.parse_args()

    csv_path = Path(args.csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    print("========================================")
    print("Replay tracking.csv to Kafka")
    print("========================================")
    print(f"CSV path         : {csv_path}")
    print(f"Kafka bootstrap  : {args.bootstrap_server}")
    print(f"Kafka topic      : {args.topic}")
    print(f"Limit            : {args.limit if args.limit > 0 else 'ALL'}")
    print(f"Sleep            : {args.sleep} seconds")
    print("========================================")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        retries=5,
        linger_ms=10,
    )

    sent_count = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for row_number, row in enumerate(reader, start=1):
            event = build_event(row, row_number)
            event_id = event["event_id"]

            producer.send(
                args.topic,
                key=str(event_id),
                value=event
            )

            sent_count += 1

            if sent_count % 1000 == 0:
                producer.flush()
                print(f"[OK] Sent {sent_count} events...")

            if args.limit > 0 and sent_count >= args.limit:
                break

            if args.sleep > 0:
                time.sleep(args.sleep)

    producer.flush()
    producer.close()

    print("========================================")
    print(f"DONE. Total events sent: {sent_count}")
    print("========================================")


if __name__ == "__main__":
    main()
CREATE KEYSPACE IF NOT EXISTS logs
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS logs.tracking_raw (
    event_id text PRIMARY KEY,
    event_time timestamp,
    event_date text,
    event_hour int,
    custom_track text,
    uid text,
    job_id text,
    campaign_id text,
    group_id text,
    publisher_id text,
    device_type text,
    revenue double,
    bid double,
    raw_json text,
    ingested_at timestamp
);
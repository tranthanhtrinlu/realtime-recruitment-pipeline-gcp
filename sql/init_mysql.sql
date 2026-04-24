CREATE DATABASE IF NOT EXISTS etl_dw;

USE etl_dw;

CREATE TABLE IF NOT EXISTS fact_events_hourly (
    event_date DATE NOT NULL,
    event_hour INT NOT NULL,
    custom_track VARCHAR(100) NOT NULL,
    total_events BIGINT NOT NULL,
    total_bid DOUBLE DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (event_date, event_hour, custom_track)
);

CREATE TABLE IF NOT EXISTS pipeline_quality_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    check_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    message TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
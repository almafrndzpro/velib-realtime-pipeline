CREATE USER velib WITH PASSWORD 'velib';
CREATE DATABASE velib OWNER velib;

\connect velib

CREATE TABLE IF NOT EXISTS staging_stations (
    id               SERIAL PRIMARY KEY,
    station_number   INTEGER          NOT NULL,
    station_name     TEXT             NOT NULL,
    address          TEXT,
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    banking          BOOLEAN,
    bonus            BOOLEAN,
    bike_stands      INTEGER,
    available_bikes  INTEGER,
    available_stands INTEGER,
    status           TEXT,
    last_update      BIGINT,
    ingested_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    batch_id         TEXT             NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uix_staging_station_batch
    ON staging_stations(station_number, batch_id);


CREATE TABLE IF NOT EXISTS stream_events (
    id               SERIAL PRIMARY KEY,
    station_number   INTEGER     NOT NULL,
    station_name     TEXT,                        
    event_type       TEXT        NOT NULL,        
    available_bikes  INTEGER,
    available_stands INTEGER,
    status           TEXT,
    received_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    kafka_offset     BIGINT
);

CREATE INDEX IF NOT EXISTS idx_stream_events_station
    ON stream_events(station_number, received_at DESC);

CREATE TABLE IF NOT EXISTS analytics_station_hourly (
    station_number   INTEGER          NOT NULL,
    station_name     TEXT,
    latitude         DOUBLE PRECISION,
    longitude        DOUBLE PRECISION,
    hour_bucket      TIMESTAMPTZ      NOT NULL,
    avg_bikes        NUMERIC(6,2),
    min_bikes        INTEGER,
    max_bikes        INTEGER,
    avg_stands       NUMERIC(6,2),
    availability_pct NUMERIC(5,2),
    n_snapshots      INTEGER,
    PRIMARY KEY (station_number, hour_bucket)
);

CREATE OR REPLACE VIEW v_stations_current AS
SELECT DISTINCT ON (station_number)
    station_number,
    station_name,
    address,
    latitude,
    longitude,
    banking,
    bike_stands,
    available_bikes,
    available_stands,
    status,
    CASE
        WHEN bike_stands = 0 THEN NULL
        ELSE ROUND(available_bikes::NUMERIC / bike_stands * 100, 1)
    END AS fill_pct,
    ingested_at
FROM staging_stations
ORDER BY station_number, ingested_at DESC;

CREATE OR REPLACE VIEW v_alert_empty AS
SELECT station_number, station_name, address, latitude, longitude,
       available_bikes, bike_stands, fill_pct, ingested_at
FROM v_stations_current
WHERE fill_pct IS NOT NULL AND fill_pct < 15
ORDER BY fill_pct ASC;

CREATE OR REPLACE VIEW v_alert_full AS
SELECT station_number, station_name, address, latitude, longitude,
       available_bikes, bike_stands, fill_pct, ingested_at
FROM v_stations_current
WHERE fill_pct IS NOT NULL AND fill_pct > 85
ORDER BY fill_pct DESC;

CREATE OR REPLACE VIEW v_kpis AS
SELECT
    COUNT(*)                                  AS total_stations,
    SUM(available_bikes)                      AS total_bikes_available,
    SUM(available_stands)                     AS total_stands_available,
    SUM(bike_stands)                          AS total_capacity,
    ROUND(AVG(fill_pct), 1)                   AS avg_fill_pct,
    COUNT(*) FILTER (WHERE status = 'OPEN')   AS stations_open,
    COUNT(*) FILTER (WHERE fill_pct < 15)     AS stations_nearly_empty,
    COUNT(*) FILTER (WHERE fill_pct > 85)     AS stations_nearly_full,
    MAX(ingested_at)                          AS last_refresh
FROM v_stations_current;

GRANT ALL ON ALL TABLES    IN SCHEMA public TO velib;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO velib;

# Setup Guide — Vélib' Real-Time Pipeline

This guide explains how to run the project on your local machine from scratch.

---

## Prerequisites

Make sure the following tools are installed before starting:

| Tool | Version | Check |
|---|---|---|
| Docker Desktop | 24+ | `docker --version` |
| Docker Compose | 2.20+ | `docker compose version` |

Docker Desktop includes Docker Compose. Download it at: https://www.docker.com/products/docker-desktop

No Python installation is required on your machine — everything runs inside Docker containers.

---

## Project Structure

```
velib-pipeline/
├── docker-compose.yml          # Defines all services (Postgres, Kafka, Airflow, Dashboard)
├── sql/
│   └── init.sql                # Creates all database tables and views on first startup
├── etl/
│   ├── velib_etl.py            # Fetches data from the API and loads it into PostgreSQL + Kafka
│   └── velib_mock_data.py      # Fallback data generator if the API is unavailable
├── streaming/
│   └── consumer.py             # Reads Kafka messages and writes events to PostgreSQL
├── airflow/
│   └── dags/
│       └── velib_pipeline_dag.py  # Airflow DAG — runs the full pipeline every 5 minutes
├── dashboard/
│   └── app.py                  # Streamlit dashboard — real-time visualizations
├── README.md
└── SETUP.md                    # This file
```

---

## Starting the Project

### Step 1 — Clone or download the project

```bash
cd ~/Downloads
# If using git:
git clone <your-repo-url> velib-pipeline
cd velib-pipeline

# If using the zip file:
unzip velib-pipeline.zip
cd velib-pipeline
```

### Step 2 — Start all services

```bash
docker compose up -d
```

This starts 8 containers in the correct order:
- PostgreSQL (database)
- Zookeeper + Kafka (message broker)
- kafka-init (creates the Kafka topics)
- airflow-init (sets up Airflow database and admin user)
- airflow-webserver + airflow-scheduler (pipeline orchestration)
- kafka-consumer (real-time event processor)
- dashboard (Streamlit web interface)

### Step 3 — Wait for initialization

Airflow takes 2 to 3 minutes to initialize on first startup. Monitor the progress with:

```bash
docker compose logs airflow-init -f
```

Wait until you see this line, then press `Ctrl+C`:
```
User admin created successfully
```

### Step 4 — Verify that the database is ready

```bash
docker compose exec postgres psql -U velib -d velib -c "\dt"
```

Expected output — you should see these 3 tables:
```
 staging_stations
 stream_events
 analytics_station_hourly
```

If the tables are missing, see the Troubleshooting section below.

### Step 5 — Trigger the first data load

Open the Airflow interface in your browser:

```
http://localhost:8081
Username: admin
Password: admin
```

1. Find the DAG named `velib_realtime_pipeline`
2. Toggle the switch to activate it (it is paused by default)
3. Click the **Trigger DAG** button (play icon on the right) to run it immediately

The pipeline fetches ~1,500 stations from the Vélib API and loads them into the database.

Alternatively, trigger it from the terminal:
```bash
docker compose exec airflow-scheduler airflow dags trigger velib_realtime_pipeline
```

### Step 6 — Verify that data arrived

```bash
docker compose exec postgres psql -U velib -d velib \
  -c "SELECT COUNT(*), MAX(ingested_at) FROM staging_stations;"
```

Expected output:
```
 count |              max
-------+-------------------------------
  1517 | 2026-06-16 14:00:00.000000+00
```

### Step 7 — Open the dashboard

```
http://localhost:8501
```

The dashboard shows the live state of all Vélib stations. It refreshes automatically every 30 seconds.

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Dashboard (Streamlit) | http://localhost:8501 | — |
| Airflow UI | http://localhost:8081 | admin / admin |
| PostgreSQL | localhost:5432 | velib / velib (database: velib) |
| Kafka | localhost:9092 | — |

---

## Useful Commands

### Check that all containers are running
```bash
docker compose ps
```

All containers should show status `running` (except `airflow-init` and `kafka-init` which exit after setup).

### View logs for a specific service
```bash
docker compose logs airflow-scheduler --tail=50
docker compose logs kafka-consumer --tail=50
docker compose logs dashboard --tail=50
```

### Verify Kafka topics were created
```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Expected output:
```
velib-alerts
velib-stations
```

### Verify data in the database
```bash
# Connect to the Vélib database
docker compose exec postgres psql -U velib -d velib

# Once connected, run:
SELECT COUNT(*) FROM v_stations_current;   -- should return ~1517
SELECT * FROM v_kpis;                      -- global KPIs
\q                                          -- exit
```

### Manually run the ETL (without Airflow)
```bash
docker compose exec airflow-scheduler python /opt/airflow/etl/velib_etl.py
```

---

## Stopping the Project

### Stop all containers (keeps data)
```bash
docker compose down
```

### Stop all containers and delete all data (full reset)
```bash
docker compose down -v
```

Use `-v` when you want a completely clean restart — this deletes the PostgreSQL volume and forces the database to be re-initialized from `sql/init.sql` on next startup.

---

## Troubleshooting

### "relation v_stations_current does not exist"

The database tables were not created. This happens when the PostgreSQL volume already exists from a previous run with an empty `sql/init.sql`.

Fix:
```bash
docker compose down -v   # delete the old volume
docker compose up -d     # restart from scratch
```

### Dashboard shows no data

The ETL has not run yet. Go to Airflow (http://localhost:8081), activate the DAG, and trigger it manually.

### Airflow DAG shows "health_check failed"

The API may be temporarily unavailable. Wait a few minutes and trigger the DAG again. The ETL has a fallback to simulated data if the API is unreachable.

### Port conflict (address already in use)

If ports 8081, 8501, 5432, or 9092 are already in use on your machine, edit `docker-compose.yml` and change the host-side port (the number before the colon):

```yaml
ports:
  - "8082:8080"   # change 8081 to any free port
```

### A container keeps restarting
```bash
docker compose logs <service-name> --tail=100
```

Replace `<service-name>` with `airflow-scheduler`, `kafka-consumer`, `dashboard`, etc.

---

## Data Source

All station data comes from the **Paris Open Data** platform (Opendatasoft), which publishes real-time Vélib station availability without requiring an API key.

Endpoint: `https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records`

Data is refreshed by the source approximately every 2 minutes. The pipeline fetches it every 5 minutes via the Airflow DAG.

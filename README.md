# Vélib' Metropolis — Real-Time Data Pipeline

**ETL & Pipeline Orchestration — ESILV MSc A4 | MACSIN4A2125**  
Student: [Your name] | Professor: Murali Krishna MOPIDEVI | June 2026

---

## Use Case

This pipeline ingests **real-time availability data** from all **1,500+ Vélib' Métropole stations** across Paris, transforms it, orchestrates it, and visualises it on an interactive operations dashboard.

**Problem solved:** Give network redistribution managers instant visibility into which stations are running low on bikes or are completely full — so they can dispatch trucks to the right locations before users are impacted.

**Data source:** Paris Open Data (Opendatasoft) — no API key required, updated every ~2 minutes.

---

## Architecture

```
Paris Open Data API (REST/JSON)
        │
        ▼
  [ETL Batch — Python]  ──────────────►  [Kafka topic: velib-stations]
        │                                           │
        ▼                                           ▼
[PostgreSQL — staging_stations]     [Consumer Python → stream_events]
        │
        ▼
  [ELT — SQL Views + Aggregations]
        │
        ▼
[analytics_station_hourly]
        │
        └─────────────────────────────► [Streamlit Dashboard]
                                                ▲
                                       [Apache Airflow DAG]
                                       (runs every 5 minutes)
```

See `architecture.drawio` for the full diagram.

---

## Technical Stack

| Layer | Technology | Role |
|---|---|---|
| ETL Batch | Python (requests, psycopg2) | Fetch API → clean → load staging |
| ELT | SQL views + aggregations | Analytical layer from raw data |
| Streaming | Apache Kafka + Python consumer | Real-time event stream → PostgreSQL |
| Orchestration | Apache Airflow 2.9 | DAG every 5 min, retries, task dependencies |
| Storage | PostgreSQL 15 | Staging, analytics, stream events |
| Dashboard | Streamlit + Plotly + Folium | 6 interactive visualisations |
| Infrastructure | Docker Compose | One-command local deployment |

---

## Requirements Coverage

- **ETL Batch** — API ingestion → cleaning → idempotent staging (`ON CONFLICT DO NOTHING`)
- **ELT SQL** — analytical views, hourly aggregation table (`analytics_station_hourly`)
- **Kafka Streaming** — topic `velib-stations`, Python consumer, sink to `stream_events`
- **Airflow Orchestration** — schedule `*/5 * * * *`, retries x3, task chain with dependencies
- **Real-time Dashboard** — 6 visualisations: KPI cards, distribution chart, map, dispatch list, collect list, 24h trend
- **Idempotent pipeline** — unique `batch_id` per run, SQL `ON CONFLICT` guard
- **End-to-end** — API → Kafka → PostgreSQL → Dashboard

---

## Quick Start

See [SETUP.md](SETUP.md) for full instructions.

```bash
# 1. Start all services
docker compose up -d

# 2. Wait ~2 minutes for Airflow and Kafka to initialize

# 3. Open Airflow, activate the DAG and trigger it
#    http://localhost:8081  (admin / admin)

# 4. Open the dashboard
#    http://localhost:8501
```

---

## Project Structure

```
velib-pipeline/
├── docker-compose.yml              # All services: Postgres, Kafka, Airflow, Dashboard
├── sql/
│   └── init.sql                    # PostgreSQL schema: tables, views, indexes
├── etl/
│   ├── velib_etl.py                # ETL pipeline: Extract → Transform → Load + Kafka publish
│   └── velib_mock_data.py          # Fallback: realistic simulated data if API is down
├── streaming/
│   └── consumer.py                 # Kafka consumer: reads velib-stations → stream_events
├── airflow/
│   └── dags/
│       └── velib_pipeline_dag.py   # Airflow DAG: health check → ETL → ELT → summary
├── dashboard/
│   └── app.py                      # Streamlit dashboard (operations manager view)
├── architecture.drawio             # End-to-end architecture diagram
├── README.md                       # This file
└── SETUP.md                        # Local setup guide
```

---

## Data Source

**API endpoint:**  
`https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records`

- No API key required
- ~1,517 stations across Paris and surrounding municipalities
- Updates every ~2 minutes at the source
- Airflow fetches every 5 minutes (~17,000 rows/hour in staging)

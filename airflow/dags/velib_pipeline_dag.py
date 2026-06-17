from datetime import datetime, timedelta
import json
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

POSTGRES_CONN_ID = "velib_postgres"
API_HEALTH_URL = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets"
    "/velib-disponibilite-en-temps-reel/records?limit=1"
)

default_args = {
    "owner":                     "velib-pipeline",
    "retries":                   3,
    "retry_delay":               timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(minutes=5),
    "depends_on_past":           False,
    "email_on_failure":          False,
}


def task_health_check(**ctx):
    import requests
    try:
        resp = requests.get(API_HEALTH_URL, timeout=10)
        resp.raise_for_status()
        data  = resp.json()
        count = data.get("total_count", 0)
        if count == 0:
            raise RuntimeError("API répond mais retourne 0 stations")
        print(f"✅ API OK – {count} stations disponibles")
    except Exception as e:
        raise RuntimeError(f"API Vélib inaccessible : {e}")


def task_run_etl(**ctx):
    """Lance le pipeline ETL (extract → transform → load + Kafka)."""
    sys.path.insert(0, "/opt/airflow/etl")
    from velib_etl import run_etl
    result = run_etl()
    ctx["ti"].xcom_push(key="etl_result", value=result)
    return result


def task_run_elt(**ctx):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    agg_sql = """
        INSERT INTO analytics_station_hourly (
            station_number, station_name, latitude, longitude,
            hour_bucket, avg_bikes, min_bikes, max_bikes,
            avg_stands, availability_pct, n_snapshots
        )
        SELECT
            station_number,
            MAX(station_name)                                        AS station_name,
            AVG(latitude)                                            AS latitude,
            AVG(longitude)                                           AS longitude,
            DATE_TRUNC('hour', ingested_at)                          AS hour_bucket,
            ROUND(AVG(available_bikes)::NUMERIC,  2)                 AS avg_bikes,
            MIN(available_bikes)                                     AS min_bikes,
            MAX(available_bikes)                                     AS max_bikes,
            ROUND(AVG(available_stands)::NUMERIC, 2)                 AS avg_stands,
            CASE WHEN SUM(bike_stands) = 0 THEN NULL
                 ELSE ROUND(
                     SUM(available_bikes)::NUMERIC / NULLIF(SUM(bike_stands), 0) * 100, 2
                 )
            END                                                      AS availability_pct,
            COUNT(*)                                                 AS n_snapshots
        FROM staging_stations
        WHERE ingested_at >= DATE_TRUNC('hour', NOW() - INTERVAL '1 hour')
          AND ingested_at <  DATE_TRUNC('hour', NOW())
        GROUP BY station_number, DATE_TRUNC('hour', ingested_at)
        ON CONFLICT (station_number, hour_bucket) DO UPDATE SET
            avg_bikes        = EXCLUDED.avg_bikes,
            min_bikes        = EXCLUDED.min_bikes,
            max_bikes        = EXCLUDED.max_bikes,
            avg_stands       = EXCLUDED.avg_stands,
            availability_pct = EXCLUDED.availability_pct,
            n_snapshots      = EXCLUDED.n_snapshots
    """
    hook.run(agg_sql)
    hook.run("DELETE FROM staging_stations WHERE ingested_at < NOW() - INTERVAL '7 days'")
    hook.run("DELETE FROM stream_events    WHERE received_at < NOW() - INTERVAL '24 hours'")

    return {"status": "elt_done"}


def task_send_summary(**ctx):
    ti         = ctx["ti"]
    etl_result = ti.xcom_pull(task_ids="run_etl", key="etl_result") or {}

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    kpis = hook.get_first("SELECT * FROM v_kpis")

    if kpis:
        print(
            f"\n{'='*50}\n"
            f"  Vélib Pipeline – Résumé\n"
            f"  batch_id        : {etl_result.get('batch_id', 'N/A')}\n"
            f"  Source          : {etl_result.get('source', 'N/A')}\n"
            f"  Stations        : {kpis[0]}\n"
            f"  Vélos dispo     : {kpis[1]}\n"
            f"  Taux moyen      : {kpis[4]}%\n"
            f"  Alertes vides   : {kpis[6]}\n"
            f"  Dernier refresh : {kpis[8]}\n"
            f"{'='*50}"
        )

with DAG(
    dag_id           = "velib_realtime_pipeline",
    description      = "Pipeline temps réel Vélib – ETL + ELT + Kafka",
    default_args     = default_args,
    start_date       = datetime(2026, 1, 1),
    schedule_interval= "*/2 * * * *",
    catchup          = False,
    max_active_runs  = 1,
    tags             = ["velib", "etl", "streaming", "realtime"],
) as dag:

    start = EmptyOperator(task_id="start")

    health_check = PythonOperator(
        task_id         = "health_check",
        python_callable = task_health_check,
    )

    run_etl = PythonOperator(
        task_id         = "run_etl",
        python_callable = task_run_etl,
    )

    run_elt = PythonOperator(
        task_id         = "run_elt_transform",
        python_callable = task_run_elt,
    )

    summary = PythonOperator(
        task_id         = "send_summary",
        python_callable = task_send_summary,
        trigger_rule    = TriggerRule.ALL_DONE,
    )

    end = EmptyOperator(task_id="end")

    start >> health_check >> run_etl >> run_elt >> summary >> end

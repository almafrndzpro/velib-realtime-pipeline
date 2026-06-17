import os
import json
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
POSTGRES_CONN   = os.getenv("POSTGRES_CONN", "postgresql://velib:velib@postgres:5432/velib")
TOPIC_IN        = "velib-stations"
TOPIC_ALERTS    = "velib-alerts"
GROUP_ID        = "velib-consumer-group"

THRESHOLD_EMPTY = 3   
THRESHOLD_FULL  = 3   


def classify_event(msg: dict) -> str:
    bikes  = msg.get("available_bikes",  0)
    stands = msg.get("available_stands", 0)
    if bikes <= THRESHOLD_EMPTY:
        return "alert_empty"
    if stands <= THRESHOLD_FULL:
        return "alert_full"
    return "update"


def get_db_connection():
    return psycopg2.connect(POSTGRES_CONN)


def insert_event(cur, msg: dict, event_type: str, offset: int):
    cur.execute("""
        INSERT INTO stream_events
            (station_number, station_name, event_type,
             available_bikes, available_stands, status, kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        msg.get("station_number"),
        msg.get("station_name"),       # ← ajouté
        event_type,
        msg.get("available_bikes"),
        msg.get("available_stands"),
        msg.get("status"),
        offset,
    ))


def main():
    log.info(f"Connexion à Kafka ({KAFKA_BOOTSTRAP}) …")

    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers   = KAFKA_BOOTSTRAP,
        group_id            = GROUP_ID,
        auto_offset_reset   = "latest",
        value_deserializer  = lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit  = False,
        max_poll_records    = 100,
    )

    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP,
        value_serializer  = lambda v: json.dumps(v).encode("utf-8"),
    )

    conn = get_db_connection()
    conn.autocommit = False

    log.info(f"En écoute sur le topic {TOPIC_IN} …")
    batch      = []
    BATCH_SIZE = 50

    for message in consumer:
        try:
            msg        = message.value
            event_type = classify_event(msg)
            batch.append((msg, event_type, message.offset))

            if event_type != "update":
                alert = {
                    "station_number":  msg.get("station_number"),
                    "station_name":    msg.get("station_name"),
                    "event_type":      event_type,
                    "available_bikes": msg.get("available_bikes"),
                    "available_stands":msg.get("available_stands"),
                    "timestamp":       datetime.now(timezone.utc).isoformat(),
                }
                producer.send(TOPIC_ALERTS, value=alert)
                log.info(
                    f"  ALERTE [{event_type}] station {msg.get('station_number')} "
                    f"– vélos={msg.get('available_bikes')} stands={msg.get('available_stands')}"
                )

            if len(batch) >= BATCH_SIZE:
                cur = conn.cursor()
                for m, et, off in batch:
                    insert_event(cur, m, et, off)
                conn.commit()
                consumer.commit()
                cur.close()
                log.info(f"  {len(batch)} événements écrits en base")
                batch = []

        except Exception as e:
            log.error(f"Erreur traitement message : {e}")
            conn.rollback()
            try:
                conn.close()
            except Exception:
                pass
            conn  = get_db_connection()
            conn.autocommit = False
            batch = []


if __name__ == "__main__":
    main()

import os
import json
import logging
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ETL] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

POSTGRES_CONN   = os.getenv("POSTGRES_CONN", "postgresql://velib:velib@postgres:5432/velib")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC     = "velib-stations"

API_BASE      = (
    "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets"
    "/velib-disponibilite-en-temps-reel/records"
)
LIMIT_PER_PAGE = 100


def generate_batch_id() -> str:
    return "batch_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")


# Extract 

def fetch_all_stations() -> list[dict] | None:
    """
    Récupère toutes les stations via pagination.
    Retourne None si l'API est inaccessible.
    """
    all_records = []
    offset = 0
    try:
        while True:
            url = f"{API_BASE}?limit={LIMIT_PER_PAGE}&offset={offset}"
            log.info(f"  Fetch offset={offset} → {url[:80]}…")
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            data    = r.json()
            results = data.get("results", [])
            if not results:
                break
            all_records.extend(results)
            total = data.get("total_count", 0)
            offset += len(results)
            if offset >= total:
                break
        log.info(f"API OK – {len(all_records)} stations récupérées")
        return all_records
    except Exception as e:
        log.warning(f"API indisponible : {e}")
        return None


# Transform 

def transform(raw: list[dict], batch_id: str) -> list[dict]:
    """
    Transforme les enregistrements de l'API Paris Open Data vers
    le schéma interne staging_stations.

    Champs API → schéma interne :
      stationcode          → station_number (int)
      name                 → station_name
      nom_arrondissement_communes → address
      coordonnees_geo.lat  → latitude
      coordonnees_geo.lon  → longitude
      capacity             → bike_stands
      numbikesavailable    → available_bikes
      numdocksavailable    → available_stands
      is_installed=="OUI" and is_renting=="OUI" → status="OPEN"
      duedate              → last_update (unix timestamp)
    """
    records, seen = [], set()
    for s in raw:
        code = s.get("stationcode")
        if not code or code in seen:
            continue
        seen.add(code)

        try:
            num = int(code)
        except (ValueError, TypeError):
            num = abs(hash(str(code))) % 100_000

        geo    = s.get("coordonnees_geo") or {}
        bikes  = max(0, int(s.get("numbikesavailable", 0)))
        stands = max(0, int(s.get("numdocksavailable", 0)))
        cap    = max(0, int(s.get("capacity", bikes + stands)))
        is_open = (s.get("is_installed") == "OUI" and s.get("is_renting") == "OUI")

        duedate = s.get("duedate")
        try:
            lu = int(datetime.fromisoformat(
                duedate.replace("Z", "+00:00")
            ).timestamp()) if duedate else int(datetime.now(timezone.utc).timestamp())
        except Exception:
            lu = int(datetime.now(timezone.utc).timestamp())

        records.append({
            "station_number":   num,
            "station_name":     s.get("name", f"Station {code}"),
            "address":          s.get("nom_arrondissement_communes", ""),
            "latitude":         float(geo.get("lat", 0)),
            "longitude":        float(geo.get("lon", 0)),
            "banking":          False,   # non fourni par l'API
            "bonus":            False,
            "bike_stands":      cap,
            "available_bikes":  bikes,
            "available_stands": stands,
            "status":           "OPEN" if is_open else "CLOSED",
            "last_update":      lu,
            "batch_id":         batch_id,
        })
    return records


# Load 

def load_to_postgres(records: list[dict]) -> int:
    """Chargement idempotent via ON CONFLICT DO NOTHING."""
    conn = psycopg2.connect(POSTGRES_CONN)
    cur  = conn.cursor()
    sql = """
        INSERT INTO staging_stations (
            station_number, station_name, address, latitude, longitude,
            banking, bonus, bike_stands, available_bikes, available_stands,
            status, last_update, batch_id
        ) VALUES (
            %(station_number)s, %(station_name)s, %(address)s,
            %(latitude)s, %(longitude)s,
            %(banking)s, %(bonus)s, %(bike_stands)s,
            %(available_bikes)s, %(available_stands)s,
            %(status)s, %(last_update)s, %(batch_id)s
        )
        ON CONFLICT (station_number, batch_id) DO NOTHING
    """
    psycopg2.extras.execute_batch(cur, sql, records, page_size=200)
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    log.info(f"  → {inserted} lignes insérées dans staging_stations")
    return inserted


def publish_to_kafka(records: list[dict]) -> None:
    """Publie chaque enregistrement dans le topic Kafka (best-effort)."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            acks="all",
            retries=3,
        )
        for r in records:
            msg = {**r, "published_at": datetime.now(timezone.utc).isoformat()}
            producer.send(KAFKA_TOPIC, key=r["station_number"], value=msg)
        producer.flush()
        producer.close()
        log.info(f"  → {len(records)} messages publiés sur {KAFKA_TOPIC}")
    except Exception as e:
        log.warning(f"Kafka non disponible : {e} – publication ignorée")


# Point d'entrée 

def run_etl() -> dict:
    batch_id = generate_batch_id()
    log.info(f"=== ETL Vélib démarré | batch_id={batch_id} ===")

    raw = fetch_all_stations()

    if raw is not None:
        records = transform(raw, batch_id)
        source  = "api"
    else:
        log.warning("API indisponible – utilisation des données simulées")
        from velib_mock_data import generate_mock_data
        records = generate_mock_data(batch_id)
        source  = "mock"

    log.info(f"  → {len(records)} enregistrements ({source})")
    inserted = load_to_postgres(records)
    publish_to_kafka(records)

    log.info(f"=== ETL terminé : {inserted}/{len(records)} lignes ===")
    return {
        "batch_id": batch_id,
        "total":    len(records),
        "inserted": inserted,
        "source":   source,
    }


if __name__ == "__main__":
    print(json.dumps(run_etl(), indent=2))

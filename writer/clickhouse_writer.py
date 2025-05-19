import logging
import clickhouse_connect
import pytz
import httpx
from datetime import datetime, timezone, timedelta
from config.utils import get_env_value
# from datastore.redis_store import get_all_seismic_data, clear_redis
from service.fdsn_fetch import fetch_station_tabular_data
from consume.station_latlon import STATION_LATLON

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CLICKHOUSE_HOST = get_env_value("CLICKHOUSE_HOST")
CLICKHOUSE_DATABASE = get_env_value("CLICKHOUSE_DATABASE")
CLICKHOUSE_USER = get_env_value("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = get_env_value("CLICKHOUSE_PASSWORD")

gmt7 = pytz.timezone("Asia/Jakarta")

locations = {entry["name"]: (entry["lat"], entry["lon"]) for entry in STATION_LATLON}

async def register_table_api(table_name, location, timestamp_str):
    url = "http://xp-keh:4000/catalog/register"
    data = {
        "table_name": table_name,
        "data_type": "seismic",
        "station": location,
        "date": timestamp_str,
        "latitude": locations.get(location, ("unknown", "unknown"))[0],
        "longitude": locations.get(location, ("unknown", "unknown"))[1],
    }
    logging.info(f"Sending registration request to {url} with JSON payload: {data}")

    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=data)
            if response.status_code in (200, 201):
                logging.info(f"Registered table for {table_name} successfully")
            else:
                logging.error(f"Failed to register table for location {location}. Status: {response.status_code}")
        except Exception as e:
            logging.error(f"Exception registering table for location {location}: {e}")

def format_unix_to_gmt7_string(unix_ts):
    try:
        dt_utc = datetime.utcfromtimestamp(unix_ts)
        dt_gmt7 = dt_utc.replace(tzinfo=pytz.utc).astimezone(gmt7)
        return dt_gmt7.strftime("%d-%m-%YT%H:%M:%S")
    except Exception:
        return ""
    
def safe_float(val: str) -> float:
    try:
        return float(val.replace(",", "."))
    except (ValueError, AttributeError):
        return 0.0 

async def bulk_write_to_clickhouse():
    try:
        clickhouse_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=8123,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
    except Exception as e:
        logging.error(f"❌ Failed to connect to ClickHouse: {e}")
        return

    previous_hour = datetime.now(timezone.utc) - timedelta(minutes=30)
    timestamp_str = previous_hour.strftime("%Y%m%d")

    total_inserted = 0

    for station_entry in STATION_LATLON:
        station = station_entry["name"]
        logging.info(f"📡 Processing station: {station}")

        try:
            tabular_data, _ = await fetch_station_tabular_data(station)
        except Exception as e:
            logging.error(f"❌ Fetch/transform error for {station}: {e}")
            continue

        if not tabular_data:
            logging.warning(f"⚠️ No data returned for station: {station}")
            continue

        table_name = f"seismic_{station}_{timestamp_str}"

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            dt String,
            lat Float32,
            lon Float32,
            network String,
            station String,
            channel String,
            data Int32
        ) ENGINE = MergeTree()
        ORDER BY (dt, channel)
        """

        try:
            clickhouse_client.command(create_table_query)
        except Exception as e:
            logging.error(f"❌ Failed to create table {table_name}: {e}")
            continue

        data_to_insert = [
            (
                row["dt"],
                row["lat"],
                row["lon"],
                row["network"],
                row["station"],
                row["channel"],
                row["data"]
            )
            for row in tabular_data
        ]

        try:
            clickhouse_client.insert(table_name, data_to_insert)
            logging.info(f"✅ Inserted {len(data_to_insert)} records into {table_name}")
            total_inserted += len(data_to_insert)
        except Exception as e:
            logging.error(f"❌ Insert failed for {station} into {table_name}: {e}")
            continue

        try:
            await register_table_api(table_name, station, timestamp_str)
        except Exception as e:
            logging.error(f"❌ Failed to register table {table_name} for {station}: {e}")

    if total_inserted > 0:
        logging.info(f"📦 Finished uploading total {total_inserted} records to ClickHouse.")
    else:
        logging.info("⚠️ No records were inserted to ClickHouse.")

    clickhouse_client.close()

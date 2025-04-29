import logging
import clickhouse_connect
import pytz
from datetime import datetime, timezone
from config.utils import get_env_value
from datastore.redis_store import get_all_seismic_data, clear_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CLICKHOUSE_HOST = get_env_value("CLICKHOUSE_HOST")
CLICKHOUSE_DATABASE = get_env_value("CLICKHOUSE_DATABASE")
CLICKHOUSE_USER = get_env_value("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = get_env_value("CLICKHOUSE_PASSWORD")

gmt7 = pytz.timezone("Asia/Jakarta")

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
        logging.error(f"Failed to connect to ClickHouse: {e}")
        return

    data = await get_all_seismic_data()
    
    if not data:
        logging.info("No new data to write to ClickHouse.")
        return

    timestamp_str = datetime.now(gmt7).strftime("%Y%m%d_%H%M")
    table_name = f"seismic_{timestamp_str}"

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
    clickhouse_client.command(create_table_query)

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
        for row in data
    ]

    utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S GMT+0")
    logging.info(f"Uploading {len(data)} records to ClickHouse at {utc_now}")

    clickhouse_client.insert(table_name, data_to_insert)

    await clear_redis()  
    logging.info(f"Uploaded {len(data)} records to ClickHouse and cleared Redis.")

    clickhouse_client.close()

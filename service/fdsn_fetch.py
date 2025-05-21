import logging
import time
from datetime import timedelta, datetime, timezone
from obspy import UTCDateTime
from obspy.clients.fdsn import Client
from obspy.clients.fdsn.header import FDSNException
from consume.station_latlon import STATION_LATLON
from concurrent.futures import ThreadPoolExecutor
import asyncio

STATIONS = [entry["name"] for entry in STATION_LATLON]
CHANNELS = ["BHZ", "BHN", "BHE"]
DATA_LENGTH = 12000
FDSN_URL = "GEOFON"
MAX_FETCH_DURATION = 300
RETRY_WAIT = 5

STATION_DICT = {entry["name"]: {"lat": entry["lat"], "lon": entry["lon"]} for entry in STATION_LATLON}

logger = logging.getLogger("seismic_fetch")
client = Client(base_url=FDSN_URL)
executor = ThreadPoolExecutor()

def get_time_window():
    now = datetime.now(timezone.utc)
    floored_minute = (now.minute // 5) * 5
    now_floored = now.replace(minute=floored_minute, second=0, microsecond=0)
    end_time = UTCDateTime(now_floored - timedelta(minutes=20))
    start_time = UTCDateTime(now_floored - timedelta(minutes=30))
    return start_time, end_time

def _fetch_station_data_sync(station_code, start_time, end_time):
    bulk = [("GE", station_code, "*", ch, start_time, end_time) for ch in CHANNELS]
    start_attempt = time.monotonic()
    attempt_count = 0

    while True:
        attempt_count += 1
        try:
            t0 = time.perf_counter()
            stream = client.get_waveforms_bulk(bulk=bulk)
            fetch_duration = time.perf_counter() - t0

            if not stream:
                logger.warning(f"[{station_code}] No data returned.")
                return None

            total_samples = sum(len(tr.data) for tr in stream)
            logger.info(f"[{station_code}] Fetched {total_samples} samples in {fetch_duration:.2f}s")

            station_data = {}
            for tr in stream:
                chan = tr.stats.channel
                tr.trim(starttime=start_time, endtime=end_time)
                samples = tr.data.tolist()
                if len(samples) >= DATA_LENGTH:
                    samples = samples[:DATA_LENGTH]
                else:
                    samples += [0] * (DATA_LENGTH - len(samples))
                station_data[chan] = {
                    "samples": samples,
                    "sampling_rate": tr.stats.sampling_rate
                }

            for ch in CHANNELS:
                if ch not in station_data:
                    station_data[ch] = {
                        "samples": [0] * DATA_LENGTH,
                        "sampling_rate": 1.0
                    }

            return station_data

        except Exception as e:
            if "502" in str(e):
                if time.monotonic() - start_attempt > MAX_FETCH_DURATION:
                    logger.warning(f"[{station_code}] Gave up after 5 minutes of 502 errors.")
                    return None
                time.sleep(RETRY_WAIT)
                continue
            else:
                logger.error(f"[{station_code}] Fetch error: {e}")
                return None

def _transform_to_tabular(station, chans, start_time):
    t0 = time.perf_counter()
    latlon = STATION_DICT.get(station, {"lat": None, "lon": None})
    output = []

    for ch, info in chans.items():
        samples = info["samples"]
        sr = info["sampling_rate"]
        interval = 1.0 / sr
        for i, val in enumerate(samples):
            dt_object = start_time + timedelta(seconds=i * interval)
            dt_unix_ms = int(dt_object.timestamp * 1000)
            dt_format = str(dt_unix_ms)[:10]
            timestamp = dt_object.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            output.append({
                "dt": dt_unix_ms,
                "dt_format": dt_format,
                "timestamp": timestamp,
                "lat": latlon["lat"],
                "lon": latlon["lon"],
                "network": "GE",
                "station": station,
                "channel": ch,
                "data": val
            })

    logger.info(f"[{station}] Transformed {len(output)} records in {time.perf_counter() - t0:.2f}s")
    return output

async def fetch_station_tabular_data(station: str):
    """Async wrapper to fetch and transform seismic data for a single station."""
    start_time, end_time = get_time_window()

    start_fmt = datetime.fromtimestamp(start_time.timestamp, tz=timezone.utc).strftime("%Y-%m-%d_%H.%M.%S")
    end_fmt = datetime.fromtimestamp(end_time.timestamp, tz=timezone.utc).strftime("%Y-%m-%d_%H.%M.%S")
    logger.info(f"[{station}] Fetching from FDSN: {start_fmt} until {end_fmt}")

    loop = asyncio.get_event_loop()
    raw_data = await loop.run_in_executor(executor, _fetch_station_data_sync, station, start_time, UTCDateTime(start_time + timedelta(minutes=10)))

    if raw_data is None:
        logger.warning(f"[{station}] No data fetched.")
        return None, start_time

    transformed = _transform_to_tabular(station, raw_data, start_time)
    return transformed, start_time

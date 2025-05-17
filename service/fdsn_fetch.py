# service/fdsn_fetch.py

import json
from datetime import datetime, timedelta, timezone
from obspy import UTCDateTime
from obspy.clients.fdsn import Client

STATIONS = ["PMBI"]
CHANNELS = ["BHZ", "BHN", "BHE"]
DATA_LENGTH = 300
FDSN_URL = "GEOFON"

client = Client(base_url=FDSN_URL)

def get_time_window():
    now = datetime.now(timezone.utc)
    floored_minute = (now.minute // 5) * 5
    now_floored = now.replace(minute=floored_minute, second=0, microsecond=0)
    end_time = UTCDateTime(now_floored - timedelta(minutes=25))
    start_time = UTCDateTime(now_floored - timedelta(minutes=30))
    return start_time, end_time

def fetch_seismic_data(station_code):
    start_time, end_time = get_time_window()
    data = {}
    try:
        bulk = [
            ("GE", station_code, "*", ch, start_time, end_time)
            for ch in CHANNELS
        ]
        stream = client.get_waveforms_bulk(bulk=bulk)
        if not stream:
            return {"error": f"No waveform data for station {station_code}"}

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

        return transform_by_station({station_code: station_data}, start_time)

    except Exception as e:
        return {"error": str(e)}

def transform_by_station(raw_data, start_time):
    out = []
    for station, chans in raw_data.items():
        station_record = {"station": station}
        for ch, info in chans.items():
            samples = info["samples"]
            sr = info["sampling_rate"]
            interval = 1.0 / sr

            timestamps = [
                (start_time + timedelta(seconds=i * interval)).strftime('%H:%M:%S.%f')[:-3]
                for i in range(len(samples))
            ]

            station_record[ch] = [
                {"time": ts, "data": val}
                for ts, val in zip(timestamps, samples)
            ]

        out.append(station_record)
    return out

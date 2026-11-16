
"""AirSight live air-quality data using Open-Meteo."""
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import threading
import time

import requests

AIR_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

# City, state, latitude, longitude
CITIES = [
    ("Delhi", "Delhi", 28.6139, 77.2090),
    ("Mumbai", "Maharashtra", 19.0760, 72.8777),
    ("Jaipur", "Rajasthan", 26.9124, 75.7873),
    ("Faridabad", "Haryana", 28.4089, 77.3178),
    ("Chennai", "Tamil Nadu", 13.0827, 80.2707)
]

POLLUTANTS = {
    "PM2.5": "pm2_5",
    "PM10": "pm10",
    "CO": "carbon_monoxide",
    "NO2": "nitrogen_dioxide",
    "SO2": "sulphur_dioxide",
    "O3": "ozone",
    "US AQI": "us_aqi",
}

_cache = {}
_cache_lock = threading.Lock()
CACHE_SECONDS = 300


def _fetch_batch(cities):
    """Fetch up to 40 locations in one request."""
    params = {
        "latitude": ",".join(str(c[2]) for c in cities),
        "longitude": ",".join(str(c[3]) for c in cities),
        "current": ",".join(POLLUTANTS.values()),
        "hourly": ",".join(POLLUTANTS.values()),
        "forecast_days": 2,
        "timezone": "Asia/Kolkata",
    }
    
    # ADD THIS LINE: Pause for 1.5 seconds to prevent 429 Rate Limit errors
    time.sleep(2) 
    
    response = requests.get(AIR_URL, params=params, timeout=(10, 30))
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, list) else [data]


def _num(value):
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _at(values, index):
    try:
        return _num(values[index])
    except (IndexError, TypeError):
        return None


def fetch_live_stations(city=None, limit=300):
    """Compatible with the existing AirSight backend function."""
    cache_key = (city.strip().casefold() if city else "*", int(limit))
    now = time.time()

    with _cache_lock:
        cached = _cache.get(cache_key)
        if cached and now - cached[0] < CACHE_SECONDS:
            return cached[1]

    selected = CITIES
    if city:
        query = city.strip().casefold()
        selected = [
            item for item in CITIES
            if item[0].casefold() == query
            or query in item[0].casefold()
            or query == item[1].casefold()
        ]

    selected = selected[:max(1, min(int(limit), len(CITIES)))]

    if not selected:
        return {
            "dataset": "Open-Meteo/CAMS model-grid estimates",
            "total_records": 0,
            "records_received": 0,
            "stations_found": 0,
            "stations": [],
        }

    # Keep requests manageable; batches are fetched concurrently.
    batches = [selected[i:i + 40] for i in range(0, len(selected), 40)]
    with ThreadPoolExecutor(max_workers=1) as pool: 
        results_by_batch = list(pool.map(_fetch_batch, batches))

    stations = []
    for batch, results in zip(batches, results_by_batch):
        for index, item in enumerate(batch):
            data = results[index] if index < len(results) else {}
            current = data.get("current") or {}
            hourly = data.get("hourly") or {}
            pollutants = {}

            for label, key in POLLUTANTS.items():
                value = _num(current.get(key))
                if value is None:
                    value = _at(hourly.get(key, []), 0)
                if value is not None:
                    pollutants[label] = value

            # First available US AQI forecast value dated after today's date.
            times = hourly.get("time", [])
            aqi_values = hourly.get("us_aqi", [])
            tomorrow_aqi = None
            today = datetime.now().date().isoformat()
            for timestamp, value in zip(times, aqi_values):
                if timestamp[:10] > today and value is not None:
                    tomorrow_aqi = float(value)
                    break

            name, state, lat, lon = item
            stations.append({
                "city": name,
                "state": state,
                "country": "India",
                "station": f"{name} (Open-Meteo model grid)",
                "last_update": current.get(
                    "time", datetime.now().strftime("%Y-%m-%d %H:%M")
                ),
                "latitude": lat,
                "longitude": lon,
                "pollutants": pollutants,
                "forecast_aqi": tomorrow_aqi,
                "data_source": "Open-Meteo/CAMS model grid",
            })

    result = {
        "dataset": "Open-Meteo/CAMS model-grid estimates; not direct CPCB readings",
        "total_records": len(stations),
        "records_received": len(stations),
        "stations_found": len(stations),
        "stations": stations,
    }

    with _cache_lock:
        _cache[cache_key] = (time.time(), result)

    return result

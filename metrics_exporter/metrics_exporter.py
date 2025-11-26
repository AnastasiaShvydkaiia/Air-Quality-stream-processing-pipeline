from flask import Flask, Response
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
from pymongo import MongoClient
from datetime import datetime, timedelta

app = Flask(__name__)

# Connect to Mongo
mongo_uri = "mongodb://mongo:27017/"
client = MongoClient(mongo_uri)
db = client["aq_db"]
raw_collection = db["raw_collection"]
agg_collection = db["agg_collection"]

# Raw sensor data
raw_metrics = {
    'PM2_5_ug_m3': Gauge('pm25_raw_ug_m3', 'Latest PM2.5 (raw)'),
    'PM10_ug_m3': Gauge('pm10_raw_ug_m3', 'Latest PM10 (raw)'),
    'CO_ppm': Gauge('co_raw_ppm', 'Latest CO (raw)'),
    'NO2_ppm': Gauge('no2_raw_ppm', 'Latest NO2 (raw)'),
    'O3_ppb': Gauge('o3_raw_ppb', 'Latest O3 (raw)'),
    'temperature': Gauge('temperature_raw_celsius', 'Temperature (raw)'),
    'humidity': Gauge('humidity_raw_percent', 'Humidity (raw)'),
    'bme_pressure': Gauge('pressure_raw_hpa', 'Pressure (raw)'),
}

raw_availability = {
    k: Gauge(f"{k}_available", f"1 if {k} data is present, 0 if missing") 
    for k in raw_metrics.keys()
}

# Aggregated AQI metrics
aqi_metrics = {
    'AQI_Max': Gauge('aqi_max', 'Maximum AQI among all parameters'),
    'AQI_PM2_5': Gauge('aqi_pm25', 'AQI for PM2.5'),
    'AQI_PM10': Gauge('aqi_pm10', 'AQI for PM10'),
    'AQI_CO': Gauge('aqi_co', 'AQI for CO'),
    'AQI_NO2': Gauge('aqi_no2', 'AQI for NO2'),
    'AQI_O3': Gauge('aqi_o3', 'AQI for O3'),
}

agg_availability = {
    k: Gauge(f"{k}_available", f"1 if {k} data is present, 0 if missing") 
    for k in aqi_metrics.keys()
}

def safe_set(gauge, value):
    """Set Prometheus gauge safely with fallback for None."""
    try:
        gauge.set(float(value))
    except (TypeError, ValueError):
        gauge.set(0)

def set_metrics_with_availability(metric_dict, availability_dict, data_dict):
    for key, gauge in metric_dict.items():
        value = data_dict.get(key)
        safe_set(gauge, value)
        availability_dict[key].set(1 if value is not None else 0)

@app.route("/metrics")
def metrics():
    # Last 5 min raw data
    five_min_ago = datetime.utcnow() - timedelta(minutes=5)
    last_raw = raw_collection.find_one(
        {"event_time": {"$gte": five_min_ago}},
        sort=[("event_time", -1)]
    )
    if last_raw:
        set_metrics_with_availability(raw_metrics, raw_availability, last_raw)
    else:
        # mark all raw data as unavailable
        for gauge in raw_metrics.values():
            safe_set(gauge, 0)
        for gauge in raw_availability.values():
            gauge.set(0)

    # Last aggregated AQI
    last_agg = agg_collection.find_one(sort=[("_id", -1)])
    if last_agg:
        set_metrics_with_availability(aqi_metrics, agg_availability, last_agg)
    else:
        for gauge in aqi_metrics.values():
            safe_set(gauge, 0)
        for gauge in agg_availability.values():
            gauge.set(0)

    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9100)

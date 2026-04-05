import fastf1
import pandas as pd
import json
import time
from confluent_kafka import Producer

# 1. Setup Kafka Producer
conf = {'bootstrap.servers': '127.0.0.1:9092'}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

# 2. Load the Session
fastf1.Cache.enable_cache('cache')
print("Loading Session Data...")
session = fastf1.get_session(2026, 'Shanghai', 'Race')
session.load()

laps = session.laps.pick_driver('VER')
telemetry = laps.get_telemetry()

track_status = session.track_status

track_status = track_status.rename(columns={'Time': 'SessionTime'})

telemetry = pd.merge_asof(
    telemetry.sort_values('SessionTime'),
    track_status.sort_values('SessionTime'),
    on='SessionTime',
    direction='backward'
)

laps_for_merge = laps[['LapStartTime', 'LapNumber']].dropna().rename(columns={'LapStartTime': 'SessionTime'})
telemetry = pd.merge_asof(
    telemetry.sort_values('SessionTime'),
    laps_for_merge.sort_values('SessionTime'),
    on='SessionTime',
    direction='backward'
)

telemetry['LapNumber'] = telemetry['LapNumber'].fillna(1)

if 'Status' not in telemetry.columns:
    telemetry = pd.merge_asof(
        telemetry.sort_values('SessionTime'),
        track_status[['SessionTime', 'Status']].sort_values('SessionTime'),
        on='SessionTime',
        direction='backward'
    )

# ==========================================
# TIME TRAVEL : Skip to the end of the race
# Uncomment the line below to fast-forward
# ==========================================
#telemetry = telemetry[telemetry['LapNumber'] >= 40]

print("Starting full Grand Prix live stream simulation...")
print("-" * 50)

# 4. The Live Stream Loop
for index, row in telemetry.iterrows():
    
    # We now dynamically pull the exact track status for this specific millisecond
    payload = {
        'driver': 'VER',
        'speed_kmh': row['Speed'],
        'rpm': row['RPM'],
        'gear': row['nGear'],
        'throttle': row['Throttle'],
        'brake': int(row['Brake']), 
        'status': int(row['Status']),
        'lap': int(row['LapNumber'])
    }
    
    producer.produce(
        'f1-telemetry', 
        key='VER', 
        value=json.dumps(payload), 
        callback=delivery_report
    )
    
    producer.poll(0)
    
    # Terminal Logging (Only print every 10th row to keep terminal readable)
    if index % 10 == 0:
        flag_type = "GREEN" if payload['status'] == 1 else "SC/YELLOW"
        print(f"Streaming -> Speed: {payload['speed_kmh']} km/h | Status: {flag_type}")
    
    # Simulate a real-time data feed (10 updates per second)
    time.sleep(0.01)

producer.flush()
print("Simulation Complete.")
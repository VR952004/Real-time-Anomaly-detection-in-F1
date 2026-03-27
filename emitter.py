import fastf1
import time
import json

# 1. Setup the cache so we don't redownload gigabytes of data every run
fastf1.Cache.enable_cache('cache')

print("Downloading session data... this might take a minute on the first run.")
# 2. Loading a session
session = fastf1.get_session(2026, 'Shanghai', 'R')
session.load()

# 3. Fetching telemetry
print("Extracting Max Verstappen's telemetry...")
laps = session.laps.pick_driver('VER')
fastest_lap = laps.pick_fastest()
telemetry = fastest_lap.get_telemetry()

print("Starting live stream simulation...")
print("-" * 40)

# 4. The Streaming Loop
for index, row in telemetry.iterrows():
    
    # Package the specific data points we care about into a dictionary
    payload = {
        "timestamp": str(row['Date']),
        "driver": "VER",
        "speed_kmh": int(row['Speed']),
        "rpm": int(row['RPM']),
        "gear": int(row['nGear']),
        "throttle": int(row['Throttle']),
        "brake": bool(row['Brake'])
    }
    
    # Converting to JSON (the standard format for Kafka and streaming systems)
    json_payload = json.dumps(payload)
    
    # Emitting the data to the console
    print(json_payload)
    
    # Pause for 100 milliseconds to simulate a live stream of data
    time.sleep(0.1)
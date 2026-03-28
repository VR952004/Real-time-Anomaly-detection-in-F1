import fastf1
import time
import json
from confluent_kafka import Producer

# Setup the cache so we don't redownload gigabytes of data every run
fastf1.Cache.enable_cache('cache')

print("Downloading session data... this might take a minute on the first run.")
# Loading a session
session = fastf1.get_session(2026, 'Shanghai', 'R')
session.load()

# Fetching telemetry
print("Extracting Max Verstappen's telemetry...")
laps = session.laps.pick_driver('VER')
telemetry = laps.get_telemetry()

print("Starting live stream simulation...")
print("-" * 40)

# Connect to the Kafka server we just started in Docker
conf = {'bootstrap.servers': '127.0.0.1:9092'}
producer = Producer(conf)
topic_name = 'f1-telemetry'

#The Streaming Loop
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
    
    # Send the payload to the Kafka topic
    producer.produce(topic_name, value=json_payload.encode('utf-8'))
    producer.poll(0) # This cleans up the internal queue
    print(f"Sent to Kafka: {json_payload}")
    
    # Pause for 100 milliseconds to simulate a live stream of data
    time.sleep(0.1)
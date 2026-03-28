import json
from confluent_kafka import Consumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# 1. Connect to InfluxDB (Using the credentials from docker-compose)
token = "my-super-secret-auth-token"
org = "f1-project"
url = "http://localhost:8086"
bucket = "telemetry"

print("Connecting to InfluxDB...")
db_client = InfluxDBClient(url=url, token=token, org=org)
write_api = db_client.write_api(write_options=SYNCHRONOUS)

# 2. Connect to Kafka
conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': 'f1-telemetry-group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['f1-telemetry'])

print("Listening to Kafka and routing data to InfluxDB...")
print("-" * 50)

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None or msg.error():
            continue
            
        payload = json.loads(msg.value().decode('utf-8'))
        
        # The Anomaly Rule
        is_anomaly = payload['gear'] == 8 and payload['speed_kmh'] < 100
        
        # 3. Package the data for the Time-Series Database
        point = (
            Point("car_telemetry")
            .tag("driver", payload.get('driver', 'VER'))
            .field("speed", payload['speed_kmh'])
            .field("rpm", payload['rpm'])
            .field("gear", payload['gear'])
            .field("is_anomaly", int(is_anomaly)) # Save as 1 (True) or 0 (False)
        )
        
        # 4. Write to the database
        write_api.write(bucket=bucket, org=org, record=point)
        
        if is_anomaly:
            print(f"ANOMALY SAVED TO DB -> Speed: {payload['speed_kmh']} in Gear {payload['gear']}")
        else:
            print(f"Saved to DB -> Speed: {payload['speed_kmh']} km/h")

except KeyboardInterrupt:
    print("\nShutting down cleanly...")
finally:
    consumer.close()
    db_client.close()
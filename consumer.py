import json
import joblib
import warnings
import uuid
from confluent_kafka import Consumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# 1. Load the Model
try:
    model = joblib.load('f1_anomaly_model.pkl')
    print("AI Model loaded.")
except FileNotFoundError:
    print("ERROR: f1_anomaly_model.pkl not found!")
    exit()

warnings.filterwarnings("ignore", category=UserWarning)

# 2. Connect to InfluxDB (Synchronous / One-by-One)
token = "my-super-secret-auth-token" 
org = "f1-project"
url = "http://localhost:8086"
bucket = "telemetry"
db_client = InfluxDBClient(url=url, token=token, org=org)
write_api = db_client.write_api(write_options=SYNCHRONOUS) 

# 3. Connect to Kafka (Randomized ID to prevent offset getting stuck)
conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': f"f1-basic-{uuid.uuid4()}",
    'auto.offset.reset': 'latest'
}
consumer = Consumer(conf)
consumer.subscribe(['f1-telemetry'])

print("Running. Waiting for Emitter...")
print("-" * 50)

# 4. The Loop
try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None or msg.error():
            continue
            
        try:
            payload = json.loads(msg.value().decode('utf-8'))
        except:
            continue
            
        if 'status' not in payload:
            continue
            
        # --- ML Inference ---
        features = [[
            payload['speed_kmh'], payload['rpm'], payload['gear'], 
            payload['throttle'], payload['brake'], payload['status']
        ]]
        
        prediction = model.predict(features)
        is_anomaly = 1 if prediction[0] == -1 else 0
        
        # --- Save to DB ---
        point = (
            Point("car_telemetry")
            .tag("driver", payload.get('driver', 'VER'))
            .field("speed", payload['speed_kmh'])
            .field("rpm", payload['rpm'])
            .field("gear", payload['gear'])
            .field("throttle", payload['throttle'])
            .field("brake", payload['brake'])
            .field("status", payload['status'])
            .field("is_anomaly", is_anomaly)
            .field("lap", payload['lap'])
        )
        
        write_api.write(bucket=bucket, org=org, record=point)

except KeyboardInterrupt:
    print("\nShutting down cleanly...")
finally:
    consumer.close()
    db_client.close()
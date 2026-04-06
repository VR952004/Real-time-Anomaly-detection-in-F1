import json
import joblib
import warnings
import uuid
import time
from confluent_kafka import Consumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS

try:
    model = joblib.load('f1_anomaly_model.pkl')
    print("AI Model loaded.")
except FileNotFoundError:
    print("ERROR: f1_anomaly_model.pkl not found!")
    exit()

warnings.filterwarnings("ignore", category=UserWarning)

token = "my-super-secret-auth-token" 
org = "f1-project"
url = "http://localhost:8086"
bucket = "telemetry"
db_client = InfluxDBClient(url=url, token=token, org=org)
write_api = db_client.write_api(write_options=ASYNCHRONOUS) 

conf = {
    'bootstrap.servers': '127.0.0.1:9092',
    'group.id': f"f1-basic-{uuid.uuid4()}",
    'auto.offset.reset': 'latest'
}
consumer = Consumer(conf)
consumer.subscribe(['f1-telemetry'])

print("Running. Waiting for Emitter...")
print("-" * 50)

try:
    #Uncomment in case you wish to find the latency between emitter and data being stored in the db
    #msg_count = 0
    
    while True:
        msgs = consumer.consume(num_messages=50, timeout=1.0)
        
        if not msgs:
            continue
            
        features_batch = []
        payloads = []
        points = []
        
        # Parse the entire batch
        for msg in msgs:
            if msg.error():
                continue
            try:
                payload = json.loads(msg.value().decode('utf-8'))
                if 'status' not in payload:
                    continue
                    
                features_batch.append([
                    payload['speed_kmh'], payload['rpm'], payload['gear'], 
                    payload['throttle'], payload['brake'], payload['status']
                ])
                payloads.append(payload)
            except:
                continue
        
        if not features_batch:
            continue
            
        predictions = model.predict(features_batch)
        
        for i, payload in enumerate(payloads):
            is_anomaly = 1 if predictions[i] == -1 else 0
            
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
            points.append(point)
            
        write_api.write(bucket=bucket, org=org, record=points)

        # --- CALCULATE AND PRINT LATENCY ---
        # We only need to check the latency of the newest message in the batch
        #last_payload = payloads[-1]
        #if 'emit_timestamp' in last_payload:
        #    process_end_time = time.time()
        #    latency_ms = (process_end_time - last_payload['emit_timestamp']) * 1000
            
        #    msg_count += len(payloads)
        #   print(f"Processed batch of {len(payloads):02d} | Total Rows: {msg_count:04d} | Current Latency: {latency_ms:.2f} ms")

except KeyboardInterrupt:
    print("\nShutting down cleanly...")
finally:
    consumer.close()
    db_client.close()
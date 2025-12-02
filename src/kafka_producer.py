from db_utils import load_from_db
from kafka import KafkaProducer
import json
import time
import pandas as pd


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_data = pd.read_csv("../stream.csv")
print("Sending data to Kafka...")

for index, row in csv_data.iterrows():
    message = row.to_dict()
    producer.send('55_2803_55_10541_Topic', value=message)
    print(f"Sent: {message}")
    time.sleep(0.3)
    
producer.send('55_2803_55_10541_Topic', value='EOF')
print("Sent EOF message.")
print("Stream ended, closing producer...")


producer.close()

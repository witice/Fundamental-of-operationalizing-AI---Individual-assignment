import logging
import json
import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from ucimlrepo import fetch_ucirepo

# Setup logging
logging.basicConfig(
    filename="producer.log",  # Log to a file
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Fetch the UCI Air Quality dataset
try:
    logging.info("Fetching UCI dataset...")
    air_quality = fetch_ucirepo(id=360)
    X = air_quality.data.features
    # y = air_quality.data.targets
    data_records = X.to_dict(orient="records")  # Convert DataFrame rows to JSON records
    logging.info(f"Dataset loaded successfully with {len(data_records)} records.")
except Exception as e:
    logging.error(f"Error fetching dataset: {e}")
    raise SystemExit("Failed to load dataset. Exiting.")  # Stop execution if dataset fails


# Convert the dataset to a list of JSON records
data_records = X.to_dict(orient="records")  # Convert DataFrame rows to dictionaries

# Initialize the Kafka producer
# - 'bootstrap_servers' defines Kafka server(s)
# - 'value_serializer' converts data to JSON and encodes it to bytes
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5  # Automatically retry if Kafka is temporarily unavailable
    )
    logging.info("Kafka producer initialized successfully.")
except KafkaError as e:
    logging.error(f"Error initializing Kafka Producer: {e}")
    raise SystemExit("Failed to connect to Kafka. Exiting.")

# Function to send messages to the Kafka topic
def send_message():
    logging.info("Starting to send messages...")
    for idx, record in enumerate(data_records):
        try:
            future = producer.send('air_quality_data', record)
            result = future.get(timeout=10)  # Wait for send confirmation
            logging.info(f"Message {idx+1}/{len(data_records)} sent successfully.")
        except KafkaError as e:
            logging.error(f"Failed to send message {idx+1}: {e}")
        time.sleep(0.5)  # Simulate streaming with delay

if __name__ == '__main__':
    send_message()
    
    # Flush ensures all buffered messages are sent to Kafka before continuing
    producer.flush()
    
    # Close the producer to free resources, ensures flush is called
    producer.close()

    logging.info("Producer finished sending messages and closed.")

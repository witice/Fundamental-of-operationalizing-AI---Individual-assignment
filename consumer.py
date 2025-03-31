import logging
import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Setup logging
logging.basicConfig(
    filename="consumer.log",  # Log to a file
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialize the Kafka consumer
# - 'test_topic' is the topic to subscribe to
# - 'bootstrap_servers' defines the Kafka server(s) to connect to
# - 'auto_offset_reset' controls where to start reading (earliest or latest)
# - 'enable_auto_commit' automatically commits the offset after consuming
# - 'value_deserializer' converts the received bytes into a Python dictionary using JSON
def create_consumer():
    try:
        consumer = KafkaConsumer(
            'air_quality_data',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',  # Start reading from the beginning if no offset is stored
            enable_auto_commit=True,  # Automatically commit the message offset after it's read
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserialize JSON messages
        )
        logging.info("Kafka consumer initialized successfully.")
        return consumer
    except KafkaError as e:
        logging.exception("Error initializing Kafka Consumer.")
        raise SystemExit("Exiting due to Kafka consumer initialization failure.")

# Function to consume messages from the topic
def consume_message(consumer):
    logging.info("Starting Kafka consumer...")
    data_list = []  # List to accumulate message data
    
    try:
        for message in consumer:
            try:
                logging.info(f"Received message: {message.value}")
                data_list.append(message.value)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode message {message.value}: {e}")
    except KafkaError:
        logging.exception("Kafka error occurred while consuming messages.")
    except KeyboardInterrupt:
        logging.warning("Consumer stopped manually.")
    finally:
        logging.info("Closing Kafka consumer.")
        consumer.close()
        
        # Convert list of JSON objects to DataFrame
        df = pd.DataFrame(data_list)
        logging.info(f"Stored {len(data_list)} messages in DataFrame")
        print(df)
        return df

if __name__ == '__main__':
    kafka_consumer = create_consumer()
    df = consume_message(kafka_consumer)  # Start consuming messages and store in DataFrame

    consume_message(kafka_consumer)  # Start consuming messages


# In this script:
# - We use KafkaConsumer to consume messages from 'test_topic'.
# - json.loads() is used to deserialize the incoming messages from JSON format to Python dict.
# - Messages are printed to the console after being consumed.

import logging
import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pandas as pd
from model import *

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
            'air_quality_test_data',
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
                msg = message.value
                if isinstance(msg, dict) and msg.get('type') == 'EOF':
                    logging.info("EOF signal received. Ending message consumption.")
                    break

                logging.info(f"Received message: {msg}")
                data_list.append(msg)

                # Convert list of JSON objects to DataFrame
                df = pd.DataFrame(data_list)
                logging.info(f"Stored {len(data_list)} messages in DataFrame")
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode message {message.value}: {e}")
    except KafkaError:
        logging.exception("Kafka error occurred while consuming messages.")
    except KeyboardInterrupt:
        logging.warning("Consumer stopped manually.")
    finally:
        logging.info("Closing Kafka consumer.")
        consumer.close()

        return df

if __name__ == '__main__':
    kafka_consumer = create_consumer()
    df = consume_message(kafka_consumer)  # Start consuming messages and store in DataFrame

    # Batch-streaming data, consume data by streaming in batch, and make prediction in batch
    if df is not None and not df.empty:
        try:
            # Replace -200 in specific columns with given averages
            replacements = {
                'CO(GT)': 2.1297113289760343,
                'NOx(GT)': 242.18929150892376,
                'NO2(GT)': 112.14513729203303
            }

            for col, avg_value in replacements.items():
                if col in df.columns:
                    df[col] = df[col].replace(-200, avg_value)

            # Run prediction
            df['DateTime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].str.replace('.', ':'), format='%m/%d/%Y %H:%M:%S')
            # Extract time-based features
            df['year'] = df['DateTime'].dt.year
            df['month'] = df['DateTime'].dt.month
            df['day'] = df['DateTime'].dt.day
            df['day_of_week'] = df['DateTime'].dt.dayofweek
            df['day_of_year'] = df['DateTime'].dt.dayofyear
            df['hour'] = df['DateTime'].dt.hour
            
            # Creating lag features for CO, NOx, and Benzene
            for lag in [1, 2, 3, 6, 12, 24]:
                df[f"CO_lag_{lag}"] = df["CO(GT)"].shift(lag)
                df[f"NOx_lag_{lag}"] = df["NOx(GT)"].shift(lag)
                df[f"Benzene_lag_{lag}"] = df["C6H6(GT)"].shift(lag)

            # Creating rolling statistics (rolling mean and std deviation)
            for window in [3, 6, 12]:
                df[f"CO_roll_mean_{window}"] = df["CO(GT)"].rolling(window=window).mean()
                df[f"CO_roll_std_{window}"] = df["CO(GT)"].rolling(window=window).std()
                df[f"NOx_roll_mean_{window}"] = df["NOx(GT)"].rolling(window=window).mean()
                df[f"NOx_roll_std_{window}"] = df["NOx(GT)"].rolling(window=window).std()
                df[f"Benzene_roll_mean_{window}"] = df["C6H6(GT)"].rolling(window=window).mean()
                df[f"Benzene_roll_std_{window}"] = df["C6H6(GT)"].rolling(window=window).std()

            df.fillna(method='bfill', inplace=True)

            CO_features = ['year', 'month', 'day', 'day_of_week', 'day_of_year', 'hour', 'CO_lag_1', 'CO_lag_2', 'CO_lag_3', 'CO_lag_6', 'CO_lag_12', 'CO_lag_24', 'CO_roll_mean_3', 'CO_roll_mean_6', 'CO_roll_mean_12', 'CO_roll_std_3', 'CO_roll_std_6', 'CO_roll_std_12', 'NOx(GT)', 'C6H6(GT)']
            X_CO = df[CO_features]

            NOx_features = ['year', 'month', 'day', 'day_of_week', 'day_of_year', 'hour', 'CO_lag_1', 'CO_lag_2', 'CO_lag_3', 'CO_lag_6', 'CO_lag_12', 'CO_lag_24', 'CO_roll_mean_3', 'CO_roll_mean_6', 'CO_roll_mean_12', 'CO_roll_std_3', 'CO_roll_std_6', 'CO_roll_std_12', 'CO(GT)', 'NO2(GT)']
            X_NOx = df[NOx_features]

            C6H6_features = ['year', 'month', 'day', 'day_of_week', 'day_of_year', 'hour', 'CO_lag_1', 'CO_lag_2', 'CO_lag_3', 'CO_lag_6', 'CO_lag_12', 'CO_lag_24', 'CO_roll_mean_3', 'CO_roll_mean_6', 'CO_roll_mean_12', 'CO_roll_std_3', 'CO_roll_std_6', 'CO_roll_std_12', 'CO(GT)']
            X_C6H6 = df[C6H6_features]

            # Linear regression
            linear_regression_CO = linear_regression_train_model('CO(GT)', CO_features)
            predictions_lr_CO = model_predict(linear_regression_CO, X_CO)
            df['prediction_lr_CO'] = predictions_lr_CO

            linear_regression_NOx = linear_regression_train_model('NOx(GT)', NOx_features)
            predictions_lr_NOx = model_predict(linear_regression_NOx, X_NOx)
            df['prediction_lr_NOx'] = predictions_lr_NOx

            linear_regression_C6H6 = linear_regression_train_model('C6H6(GT)', C6H6_features)
            predictions_lr_C6H6 = model_predict(linear_regression_C6H6, X_C6H6)
            df['prediction_rf_C6H6'] = predictions_lr_C6H6

            # Random forest
            random_forest_CO = random_forest_train_model('CO(GT)', CO_features)
            predictions_rf_CO = model_predict(random_forest_CO, X_CO)
            df['prediction_rf_CO'] = predictions_rf_CO

            random_forest_NOx = random_forest_train_model('NOx(GT)', NOx_features)
            predictions_rf_NOx = model_predict(random_forest_NOx, X_NOx)
            df['prediction_rf_NOx'] = predictions_rf_NOx

            random_forest_C6H6 = random_forest_train_model('C6H6(GT)', C6H6_features)
            predictions_rf_C6H6 = model_predict(random_forest_C6H6, X_C6H6)
            df['prediction_rf_C6H6'] = predictions_rf_C6H6

            evaluate_model(linear_regression_CO, X_CO, df['CO(GT)'], "Linear regression CO")
            evaluate_model(linear_regression_NOx, X_NOx, df['NOx(GT)'], "Linear regression NOx")
            evaluate_model(linear_regression_C6H6, X_C6H6, df['C6H6(GT)'], "Linear regression C6H6")

            evaluate_model(random_forest_CO, X_CO, df['CO(GT)'], "Random forest CO")
            evaluate_model(random_forest_NOx, X_NOx, df['NOx(GT)'], "Random forest NOx")
            evaluate_model(random_forest_C6H6, X_C6H6, df['C6H6(GT)'], "Random forest C6H6")

            # Save to CSV
            df.to_csv('predicted_air_quality.csv', index=False)
            logging.info("Predictions saved to 'predicted_air_quality.csv'.")

        except Exception as e:
            logging.exception("Error during prediction or saving results.")

    #consume_message(kafka_consumer)  # Start consuming messages


# In this script:
# - We use KafkaConsumer to consume messages from 'test_topic'.
# - json.loads() is used to deserialize the incoming messages from JSON format to Python dict.
# - Messages are printed to the console after being consumed.


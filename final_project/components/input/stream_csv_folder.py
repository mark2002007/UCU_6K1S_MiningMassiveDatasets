import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import time
import argparse
from confluent_kafka import Producer
from numpy.random import exponential

from utility.spark_session import get_or_create_spark_session
from utility.change_schema import changeSchema


def csv_to_kafka(input_folder, kafka_broker, kafka_topic, distibution_scale, verbose = False):
    spark = get_or_create_spark_session()

    csv_df = spark.read.csv(
        input_folder,
        header=True,
        schema=changeSchema
    )
    csv_df = csv_df.orderBy("timestamp")
    rows = csv_df.toJSON().collect()

    # Initialize Kafka producer
    kafka_producer = Producer({
        'bootstrap.servers': kafka_broker,
        'client.id': 'wikipedia-producer-csvs'
    })

    print(f"Starting to stream {len(rows)} changes to Kafka topic '{kafka_topic}'...")

    for row in rows:
        try:
            kafka_producer.produce(kafka_topic, value=row)
            kafka_producer.flush()
            if verbose:
                print(f"Message sent to Kafka: {row[:50]}...")
        except Exception as e:
            print(f"Error producing message to Kafka: {e}")
            break
        
        interval_ms = exponential(distibution_scale)
        time.sleep(interval_ms / 1000.0)

    print("All rows sent successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send CSV rows to Kafka at random intervals.")
    parser.add_argument("--broker", help="Kafka broker URL (e.g., localhost:9092)")
    parser.add_argument("--topic", help="Kafka topic to send data to")
    parser.add_argument("--input_folder", help="Path to the folder containing CSV files")
    parser.add_argument("--distribution_scale", type=float, default=10., help="Mean and std of exponential distribution used to sleep between sending rows (in milliseconds)")
    parser.add_argument('--verbose', 
                        action='store_true', 
                        help="Log streamed messages to the chat", 
                        default=False)
    args = parser.parse_args()

    csv_to_kafka(args.input_folder, args.broker, args.topic, args.distribution_scale, args.verbose)

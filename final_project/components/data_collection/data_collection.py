import argparse
import json
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import pyspark.sql.functions as F

from utility.change_schema import changeSchema
from utility.spark_session import get_or_create_spark_session


def collect_csvs(kafka_broker: str, kafka_topic: str, sampling_freq: float, output_folder: str):
    spark = get_or_create_spark_session()

    input_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    parsed_df = input_df.select(
        F.from_json(F.col("value").cast("string"), changeSchema).alias("parsed_value")
    ).select("parsed_value.*")  # Flatten the data

    sampled_df = parsed_df.filter(
        (F.abs(F.hash(F.col("user"))) % 1000 / 1000) < sampling_freq 
    )

    query = sampled_df.writeStream \
        .format("csv") \
        .option("path", output_folder) \
        .option("header", True) \
        .trigger(processingTime="1 minute") \
        .outputMode("append") \
        .start()
    
    query.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Obtain Wikipedia changes, do sampling and save to csvs")
    parser.add_argument('--broker', required=True, help="Kafka broker address (e.g., 'localhost:9092')")
    parser.add_argument('--topic', required=True, help="Kafka topic to recieve messages")
    parser.add_argument('--sampling_freq', type=float, default=0.2, help="Sampling frequency to save to csvs")
    parser.add_argument('--dest_folder', default='./data/test_streaming_csv', help="Place to store resulting csvs")
    args = parser.parse_args()

    if args.sampling_freq < 0 or args.sampling_freq > 1:
        raise ValueError(f"Illegal sampling frequency passed: {args.sampling_freq}")

    collect_csvs(args.broker, args.topic, args.sampling_freq, args.dest_folder)

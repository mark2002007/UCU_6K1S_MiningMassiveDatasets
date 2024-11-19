import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import argparse
from pyspark.sql import functions as F

from utility.spark_session import get_or_create_spark_session
from utility.change_schema import changeSchema
from models.bloom_filter import BloomFilterBasedModel


def train_filter(kafka_broker, kafka_topic, filter_path, train_period, forget_period):
    spark = get_or_create_spark_session()

    input_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    windowed_df = input_df.withWatermark("timestamp", f"{forget_period} seconds")

    def train_and_save_model(batch_df, batch_id):
        bloomfilter = BloomFilterBasedModel(spark, fpr=0.1)
        batch_df = batch_df.withColumn('user', F.col('value').cast("string")).withColumn(
            'label', F.lit(1)
        )
        bloomfilter.fit(batch_df)

        bloomfilter.save(filter_path)
        print(f"Bloom Filter trained and saved at {filter_path} (Batch ID: {batch_id})")

    # Train the model periodically using foreachBatch
    query = windowed_df.writeStream \
        .foreachBatch(train_and_save_model) \
        .trigger(processingTime=f"{train_period} seconds") \
        .start()

    query.awaitTermination()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Train a bloom filter based on received bots")
    parser.add_argument("--broker", help="Kafka broker URL (e.g., localhost:9092)")
    parser.add_argument("--topic", help="Kafka topic to obtain bots from")
    parser.add_argument("--filter_path", help="Path to the file to save filter")
    parser.add_argument("--train_period", type=int, default=3600, help="Time between two trains, in seconds (default: 3600)")
    parser.add_argument("--forget_period", type=int, default=24*3600, help="Time after whitch the accused user is allowed to write again, in seconds (default: 24*3600)")
    args = parser.parse_args()

    train_filter(args.broker, args.topic, args.filter_path, args.train_period, args.forget_period)

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import argparse
import time
from pyspark.sql import functions as F
from threading import Thread

from utility.spark_session import get_or_create_spark_session
from utility.change_schema import changeSchema
from models.bloom_filter import BloomFilterBasedModel, BloomFilter


def periodic_update_bloom_filter(model, filter_path, reload_time_secs: int = 3600):
    while True:
        time.sleep(reload_time_secs)
        print("Reloading Bloom filter...")
        model.bloom_filter = BloomFilter.load(filter_path)
        print("Bloom filter updated.")


def inference_filter(kafka_broker, kafka_topic_in, kafka_topic_out, filter_path, reload_bloom_filter_secs):
    spark = get_or_create_spark_session()

    input_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic_in) \
        .option("startingOffsets", "latest") \
        .load()
    
    parsed_df = input_df.select(
        F.from_json(F.col("value").cast("string"), changeSchema).alias("parsed_value"), "key"
    ).select("parsed_value.*", "key")

    parsed_df = parsed_df.filter(parsed_df.user.isNotNull())
    
    bloom_filter = BloomFilterBasedModel.load(spark, filter_path)

    Thread(target=periodic_update_bloom_filter, args=(bloom_filter, filter_path, reload_bloom_filter_secs), daemon=True).start()
    
    prediction_df = bloom_filter.predict(parsed_df)
    filtered_df = prediction_df.filter(F.col("prediction") == 0).drop("prediction")

    value_df = filtered_df.withColumn(
        "value", 
        F.to_json(F.struct([col for col in filtered_df.columns if not col in ["key", "timestamp"]]))
    ).select("key", "value")

    query = value_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", kafka_topic_out) \
        .outputMode("append") \
        .start()
    
    metrics_df = prediction_df.withColumn(
        "event_time", F.from_unixtime(F.col("timestamp"))
    ).withColumn(
        "tp", F.expr("CASE WHEN bot AND prediction = 1 THEN 1 ELSE 0 END")
    ).withColumn(
        "fp", F.expr("CASE WHEN NOT bot AND prediction = 1 THEN 1 ELSE 0 END")
    ).withColumn(
        "fn", F.expr("CASE WHEN bot AND prediction = 0 THEN 1 ELSE 0 END")
    ).groupBy(
        F.window("event_time", "1 minute", "30 seconds")
    ).agg(
        F.first('bot').alias("bots"),
        F.sum("tp").alias("true_positives"),
        F.sum("fp").alias("false_positives"),
        F.sum("fn").alias("false_negatives"),
        F.count("*").alias("total_observations")
    ).withColumn(
        "accuracy", F.expr("CASE WHEN total_observations = 0 THEN 1 ELSE (total_observations - false_positives - false_negatives) / total_observations END")
    ).withColumn(
        "precision", F.expr("CASE WHEN false_positives = 0 AND true_positives = 0 THEN 1 ELSE true_positives / (true_positives + false_positives) END")
    ).withColumn(
        "recall", F.expr("CASE WHEN false_negatives = 0 AND true_positives = 0 THEN 1 ELSE true_positives / (true_positives + false_negatives) END")
    ).withColumn(
        "f1_score", F.expr("CASE WHEN precision = 0 AND recall = 0 THEN 0 ELSE 2*precision*recall/(precision + recall) END")
    )

    metrics_query = metrics_df.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("accuracy"),
        F.col("precision"),
        F.col("recall"),
        F.col("f1_score"),
        F.col("total_observations"),
        F.col("true_positives"),
        F.col("false_positives"),
        F.col("false_negatives")
    ).writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start()

    spark.streams.awaitAnyTermination()
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Filter Kafka stream from bots")
    parser.add_argument("--broker", help="Kafka broker URL (e.g., localhost:9092)")
    parser.add_argument("--topic_in", help="Kafka topic to obtain data from")
    parser.add_argument("--topic_out", help="Kafka topic to send data to")
    parser.add_argument("--filter_path", help="Path to the file containing Bloom Filter")
    parser.add_argument("--filter_reload_period", help="Period of bloom filter retraining, seconds (default 3600, meaning an hour)", type=int, default=3600)
    args = parser.parse_args()

    inference_filter(args.broker, args.topic_in, args.topic_out, args.filter_path, args.filter_reload_period)

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import argparse
from pyspark.sql import functions as F

from utility.spark_session import get_or_create_spark_session
from utility.change_schema import changeSchema
from models.emb_logreg import EmbeddingsLogReg


def inference_model(kafka_broker, kafka_topic_in, kafka_topic_out, model_path, sampling_freq):
    spark = get_or_create_spark_session()

    emb_logreg = EmbeddingsLogReg.load(model_path) 

    input_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic_in) \
        .option("startingOffsets", "latest") \
        .load()
    
    parsed_df = input_df.select(
        F.from_json(F.col("value").cast("string"), changeSchema).alias("parsed_value")
    ).select("parsed_value.*")

    user_activity_df = parsed_df.groupBy(
        F.window(F.from_unixtime(F.col("timestamp")), "1 minute"), "user"
    ).agg(F.count("*").alias("edits_per_minute")).select(
        F.col("user"), F.col("edits_per_minute")
    )

    enriched_df = parsed_df.join(user_activity_df, on="user", how="left").fillna(0, subset=["edits_per_minute"])
    enriched_df = enriched_df.withColumn(
        "text",
        F.concat(
            F.col("title"),
            F.when(F.col("comment").isNotNull(), F.col("comment")).otherwise(F.lit("NULL"))
        )
    )

    sampled_df = enriched_df.filter(
        (F.abs(F.hash(F.col("text"))) % 1000 / 1000) < sampling_freq 
    )

    prediction_df = emb_logreg.predict(sampled_df)

    metrics_df = prediction_df.withColumn(
        "event_time", F.from_unixtime(F.col("timestamp"))
    ).withColumn(
        "tp", F.expr("CASE WHEN bot AND prediction = 1 THEN 1 ELSE 0 END")
    ).withColumn(
        "fp", F.expr("CASE WHEN NOT bot AND prediction = 1 THEN 1 ELSE 0 END")
    ).withColumn(
        "fn", F.expr("CASE WHEN bot AND prediction = 0 THEN 1 ELSE 0 END")
    ).groupBy(
        F.window("event_time", "10 minutes", "5 minutes")
    ).agg(
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

    predicted_bots = prediction_df.filter(F.col("prediction") == 1).select(F.col("user").alias("value"))

    send_query = predicted_bots.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", kafka_topic_out) \
        .outputMode("append") \
        .start()

    spark.streams.awaitAnyTermination()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Found bots in Kafka stream using pre-trained model")
    parser.add_argument("--broker", help="Kafka broker URL (e.g., localhost:9092)")
    parser.add_argument("--topic_in", help="Kafka topic to obtain data from")
    parser.add_argument("--topic_out", help="Kafka topic to send bot users to")
    parser.add_argument("--model_path", help="Path to the folder containing the model")
    parser.add_argument("--sampling_freq", type=float, default=0.2, help="Sampling frequency to train the model")
    args = parser.parse_args()

    if args.sampling_freq < 0 or args.sampling_freq > 1:
        raise ValueError(f"Illegal sampling frequency passed: {args.sampling_freq}")

    inference_model(args.broker, args.topic_in, args.topic_out, args.model_path, args.sampling_freq)

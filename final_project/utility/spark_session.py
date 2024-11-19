from pyspark.sql import SparkSession


def get_or_create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("WikimediaSession") \
        .config("spark.driver.memory", "8G") \
        .config("spark.executor.memory", "3G") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "2000M") \
        .config("spark.sql.streaming.checkpointLocation", "./data/checkpoints") \
        .getOrCreate()
    
            # .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1") \

        # /home/andrii/spark-3.5.3-bin-hadoop3/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-1
        # 0_2.12:3.5.1,com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1 --driver-memory 10G components/inference/lr_inference.py

    spark.sparkContext.setLogLevel("ERROR")
    
    return spark

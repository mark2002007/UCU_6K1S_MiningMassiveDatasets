from pyspark.sql import SparkSession


def get_or_create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("WikimediaSession") \
        .config("spark.driver.memory", "12G") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "2000M") \
        .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.1") \
        .config("spark.sql.streaming.checkpointLocation", "./data/checkpoints") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    return spark

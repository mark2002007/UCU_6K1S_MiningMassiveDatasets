from pyspark.sql import SparkSession


def get_or_create_spark_session() -> SparkSession:
    spark = SparkSession.builder \
        .appName("WikimediaSession") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "2000M") \
        .config("spark.sql.streaming.checkpointLocation", "./data/checkpoints") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark

import pyspark.sql.types as T


changeSchema = T.StructType([
    T.StructField("$schema", T.StringType(), True),
    T.StructField("meta", T.StringType(), True),
    T.StructField("id", T.LongType(), True),
    T.StructField("type", T.StringType(), True),
    T.StructField("namespace", T.IntegerType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("title_url", T.StringType(), True),
    T.StructField("comment", T.StringType(), True),
    T.StructField("timestamp", T.LongType(), True),
    T.StructField("user", T.StringType(), True),
    T.StructField("bot", T.BooleanType(), True),
    T.StructField("notify_url", T.StringType(), True),
    T.StructField("minor", T.BooleanType(), True),
    T.StructField("length", T.StringType(), True),
    T.StructField("revision", T.StringType(), True),
    T.StructField("server_url", T.StringType(), True),
    T.StructField("server_name", T.StringType(), True),
    T.StructField("server_script_path", T.StringType(), True),
    T.StructField("wiki", T.StringType(), True),
    T.StructField("parsedcomment", T.StringType(), True)
])

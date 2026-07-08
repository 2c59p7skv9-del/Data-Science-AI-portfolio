from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder \
    .appName("SpotifyConsoleTest") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("person_name", StringType(), False),
    StructField("song_name", StringType(), False),
    StructField("listen_time", StringType(), False)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "spotify_songs") \
    .option("startingOffsets", "latest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()



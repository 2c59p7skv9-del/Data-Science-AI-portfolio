from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, to_timestamp, date_format

spark = SparkSession.builder \
    .appName("SpotifyToCassandra") \
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

parsed = parsed.withColumn(
    "listen_time_ts",
    to_timestamp(col("listen_time"), "yyyy-MM-dd HH:mm:ss")
)

parsed = parsed.withColumn(
    "listen_hour",
    date_format(col("listen_time_ts"), "yyyy-MM-dd HH:00")
)

songs = spark.read.csv(
    "/opt/spark/work-dir/spotify-songs.csv",
    header=True,
    inferSchema=True
)

songs = songs.select(
    col("name").alias("csv_song_name"),
    col("artists"),
    col("danceability"),
    col("tempo")
).filter(col("csv_song_name").isNotNull())

songs = songs.cache()
songs.count()

joined = parsed.join(
    songs,
    parsed.song_name == songs.csv_song_name,
    "inner"
)

final_df = joined.select(
    col("person_name"),
    col("listen_hour"),
    col("listen_time_ts").alias("listen_time"),
    col("song_name"),
    col("artists"),
    col("danceability").cast("double"),
    col("tempo").cast("double")
)

def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("spark.cassandra.connection.host", "cassandra") \
        .option("spark.cassandra.connection.port", "9042") \
        .options(table="song_listens", keyspace="spotify") \
        .save()

query = final_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, StructField

# ======================
# 1. Spark Session
# ======================
spark = SparkSession.builder \
    .appName("KafkaToHDFS_CSV") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ======================
# 2. Kafka Topic Schema (matches your JSON)
# ======================
geo_schema = StructType([
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True)
])

schema = StructType([
    StructField("idbase", StringType(), True),
    StructField("typeemplacement", StringType(), True),
    StructField("domanialite", StringType(), True),
    StructField("arrondissement", StringType(), True),
    StructField("complementadresse", StringType(), True),
    StructField("adresse", StringType(), True),
    StructField("idemplacement", StringType(), True),
    StructField("libellefrancais", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("espece", StringType(), True),
    StructField("varieteoucultivar", StringType(), True),
    StructField("circonferenceencm", DoubleType(), True),
    StructField("hauteurenm", DoubleType(), True),
    StructField("stadedeveloppement", StringType(), True),
    StructField("remarquable", StringType(), True),
    StructField("geo_point_2d", geo_schema, True)
])

# ======================
# 3. Read from Kafka
# ======================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "paris-trees-stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka value is bytes → convert to string
kafka_df_string = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON
kafka_df_parsed = kafka_df_string.select(from_json(col("json_str"), schema).alias("data")) \
                                  .select("data.*")

# Flatten geo_point_2d to lon and lat
kafka_df_flat = kafka_df_parsed.withColumn("lon", col("geo_point_2d.lon")) \
                               .withColumn("lat", col("geo_point_2d.lat")) \
                               .drop("geo_point_2d")

# ======================
# 4. Write Stream to HDFS with Header Only at Offset 0
# ======================
def write_to_hdfs(batch_df, batch_id):
    """
    Write each batch to HDFS.
    Only batch_id 0 (first offset) gets the header.
    """
    if batch_df.count() > 0:
        # Header only for the first batch (offset 0)
        include_header = (batch_id == 0)
        
        batch_df.write \
            .mode("append") \
            .option("header", str(include_header).lower()) \
            .csv("hdfs://namenode:9000/data/paris_trees/raw_csv")
        
        print(f" Batch {batch_id} written {'WITH HEADER' if include_header else 'without header'}")
    else:
        print(f" Batch {batch_id} is empty, skipping")

# Start streaming with foreachBatch
query = kafka_df_flat.writeStream \
    .foreachBatch(write_to_hdfs) \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/paris_trees_csv") \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .start()

print("Streaming job started... writing to HDFS in CSV format")
print("Header will be written ONLY for the first batch (offset 0)")

query.awaitTermination()
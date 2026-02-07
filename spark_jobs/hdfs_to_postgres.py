from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================
# CREATE SPARK SESSION
# ============================
spark = SparkSession.builder \
    .appName("HDFS → PostgreSQL Streaming") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark Session created\n")

# ============================
# DEFINE CSV SCHEMA
# ============================
schema = StructType([
    StructField("idbase", StringType()),
    StructField("type", StringType()),
    StructField("domanialite", StringType()),
    StructField("arrondissement", StringType()),
    StructField("complement_adresse", StringType()),
    StructField("adresse", StringType()),
    StructField("id_location", StringType()),
    StructField("nom", StringType()),
    StructField("genre", StringType()),
    StructField("espece", StringType()),
    StructField("variete", StringType()),
    StructField("hauteur", DoubleType()),
    StructField("circonference", DoubleType()),
    StructField("stade_developpement", StringType()),
    StructField("remarquable", StringType()),
    StructField("lon", DoubleType()),
    StructField("lat", DoubleType())
])

input_path = "hdfs://namenode:9000/data/paris_trees/raw_csv/part-*.csv"
print(f"📂 Reading from: {input_path}\n")

# ============================
# READ STREAMING DATAFRAME
# ============================
df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(input_path)

print("✅ Stream configured\n")

# ============================
# AGGREGATIONS
# ============================
# 1️⃣ Trees by arrondissement
trees_by_arrondissement = df.groupBy("arrondissement") \
    .agg(count("*").alias("total_trees")) \
    .withColumn("timestamp", current_timestamp())

# 2️⃣ Top species
top_species = df.groupBy("espece") \
    .agg(count("*").alias("count")) \
    .withColumn("timestamp", current_timestamp()) \
    .orderBy(desc("count")) \
    .limit(50)

# 3️⃣ Height statistics
height_stats = df.filter(col("hauteur").isNotNull()) \
    .groupBy("espece") \
    .agg(
        avg("hauteur").alias("avg_height"),
        max("hauteur").alias("max_height"),
        min("hauteur").alias("min_height"),
        count("*").alias("count")
    ) \
    .filter(col("count") > 5) \
    .withColumn("timestamp", current_timestamp()) \
    .orderBy(desc("avg_height"))

# 4️⃣ Remarkable trees
remarkable_by_arr = df.groupBy("arrondissement") \
    .agg(
        count("*").alias("total_trees"),
        sum(when(col("remarquable") == "OUI", 1).otherwise(0)).alias("remarkable_trees")
    ) \
    .withColumn("timestamp", current_timestamp())

# 5️⃣ Geo data
geo_data = df.filter(col("lat").isNotNull() & col("lon").isNotNull()) \
    .select("lat", "lon", "arrondissement", "espece", "hauteur", "remarquable") \
    .withColumn("timestamp", current_timestamp())

# ============================
# POSTGRES CONFIG
# ============================
pg_url = "jdbc:postgresql://postgres:5432/treesdb"
pg_properties = {
    "user": "spark",
    "password": "spark123",
    "driver": "org.postgresql.Driver"
}

# ============================
# FUNCTION TO WRITE TO POSTGRES
# ============================
def write_to_postgres(batch_df, batch_id, table_name):
    batch_df.write.jdbc(
        url=pg_url,
        table=table_name,
        mode="append",
        properties=pg_properties
    )

# ============================
# STREAMING WRITES
# ============================
q1 = trees_by_arrondissement.writeStream \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "trees_by_arrondissement")) \
    .outputMode("complete") \
    .start()

q2 = top_species.writeStream \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "trees_by_species")) \
    .outputMode("complete") \
    .start()

q3 = height_stats.writeStream \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "trees_height_stats")) \
    .outputMode("complete") \
    .start()

q4 = remarkable_by_arr.writeStream \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "trees_remarkable_stats")) \
    .outputMode("complete") \
    .start()

q5 = geo_data.writeStream \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "trees_geo_data")) \
    .outputMode("append") \
    .start()

# ============================
# CONSOLE DEBUGGING (OPTIONAL)
# ============================
console = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 20) \
    .start()

spark.streams.awaitAnyTermination()

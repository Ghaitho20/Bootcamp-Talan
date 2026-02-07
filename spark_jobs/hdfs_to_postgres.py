from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============================
# CREATE SPARK SESSION
# ============================
spark = SparkSession.builder \
    .appName("HDFS_to_Postgres_Batch") \
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

# ============================
# READ CSV FILES FROM HDFS (BATCH)
# ============================
input_path = "hdfs://namenode:9000/data/paris_trees/raw_csv/part-*.csv"
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(input_path)

if df.count() == 0:
    print("No new data in HDFS")
    exit(0)

# ============================
# AGGREGATIONS
# ============================
trees_by_arrondissement = df.groupBy("arrondissement") \
    .agg(count("*").alias("total_trees")) \
    .withColumn("timestamp", current_timestamp())

top_species = df.groupBy("espece") \
    .agg(count("*").alias("count")) \
    .withColumn("timestamp", current_timestamp()) \
    .orderBy(desc("count")) \
    .limit(50)

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

remarkable_by_arr = df.groupBy("arrondissement") \
    .agg(
        count("*").alias("total_trees"),
        sum(when(col("remarquable") == "OUI", 1).otherwise(0)).alias("remarkable_trees")
    ) \
    .withColumn("timestamp", current_timestamp())

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
def write_to_postgres(df, table_name):
    df.write.jdbc(url=pg_url, table=table_name, mode="append", properties=pg_properties)
    print(f"✅ Written {df.count()} records to {table_name}")

# ============================
# WRITE AGGREGATIONS
# ============================
write_to_postgres(trees_by_arrondissement, "trees_by_arrondissement")
write_to_postgres(top_species, "trees_by_species")
write_to_postgres(height_stats, "trees_height_stats")
write_to_postgres(remarkable_by_arr, "trees_remarkable_stats")
write_to_postgres(geo_data, "trees_geo_data")

print("✅ All data written to Postgres. Job finished.")

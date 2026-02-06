from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("HDFS → Spark Streaming → Elasticsearch") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================
# SCHEMA CSV (IMPORTANT)
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
# READ STREAM FROM HDFS
# ============================

input_path = "hdfs://localhost:9000/data/paris_trees/raw_csv/part-*"

df = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(input_path)

# Nettoyage
df = df.filter(col("idbase").isNotNull())


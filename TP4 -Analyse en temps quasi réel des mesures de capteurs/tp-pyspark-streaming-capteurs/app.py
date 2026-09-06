from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, DoubleType, TimestampType

# Creation de la session Spark
spark = SparkSession.builder \
    .appName("TP PySpark Structured Streaming - Capteurs HDFS") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definition du schema explicite des fichiers CSV
schema_capteurs = StructType([
    StructField("id", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("capteur", StringType(), True),
    StructField("valeur", DoubleType(), True),
    StructField("unite", StringType(), True)
])

# Chemins HDFS
source_path = "hdfs://namenode:8020/streaming/capteurs"
checkpoint_stats = "hdfs://namenode:8020/streaming/checkpoints/capteurs_stats"
checkpoint_alertes = "hdfs://namenode:8020/streaming/checkpoints/capteurs_alertes"

# Lecture du flux depuis HDFS
df_stream = spark.readStream \
    .option("header", "true") \
    .option("maxFilesPerTrigger", 1) \
    .schema(schema_capteurs) \
    .csv(source_path)

# Affichage du schema
df_stream.printSchema()

# Calcul des statistiques par capteur
stats_capteurs = df_stream.groupBy("capteur") \
    .agg(
        avg("valeur").alias("moyenne_valeur"),
        min("valeur").alias("valeur_min"),
        max("valeur").alias("valeur_max"),
        count("*").alias("nombre_mesures")
    )

# Detection des valeurs anormales
seuil_anomalie = 35.0

alertes = df_stream.filter(col("valeur") > seuil_anomalie) \
    .select(
        "id",
        "timestamp",
        "capteur",
        "valeur",
        "unite"
    )

# Ecriture des statistiques dans la console
query_stats = stats_capteurs.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_stats) \
    .trigger(processingTime="10 seconds") \
    .start()

# Ecriture des alertes dans la console
query_alertes = alertes.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_alertes) \
    .trigger(processingTime="10 seconds") \
    .start()

# Attendre l'arret des traitements
spark.streams.awaitAnyTermination()

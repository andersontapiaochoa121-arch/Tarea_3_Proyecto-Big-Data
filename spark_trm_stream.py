# ============================================
# Archivo: spark_trm_stream.py
# Descripción: Procesamiento en tiempo real de TRM desde Kafka
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# 1️⃣ Crear sesión de Spark con soporte Kafka
spark = SparkSession.builder     .appName("TRM_Streaming")     .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2️⃣ Definir esquema de los mensajes Kafka
schema = StructType([
    StructField("fecha", StringType()),
    StructField("valor", FloatType())
])

# 3️⃣ Leer stream desde Kafka
df_kafka = spark.readStream.format("kafka")     .option("kafka.bootstrap.servers", "localhost:9092")     .option("subscribe", "trm_data")     .option("startingOffsets", "latest")     .load()

# 4️⃣ Parsear JSON y extraer los campos
df_parsed = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 5️⃣ Calcular promedio de TRM en ventana de 1 minuto
df_agg = df_parsed.groupBy(
    window(col("fecha").cast("timestamp"), "1 minute")
).agg(avg("valor").alias("TRM_promedio_minuto"))

# 6️⃣ Escribir resultados en consola
query = df_agg.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()

# ============================================
#  Archivo: spark_trm_batch.py
#  Autor: Anderson David Tapia Ochoa
#  Proyecto: Tasa de Cambio Representativa del Mercado - Big Data UNAD
#  Descripción: Procesamiento batch de datos históricos TRM con Apache Spark
# ============================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, avg, max, min

# 1️⃣ Crear la sesión de Spark
spark = SparkSession.builder     .appName("TRM_Historico_Batch")     .config("spark.sql.shuffle.partitions", "8")     .getOrCreate()

# 2️⃣ Cargar el dataset CSV (ajusta la ruta según tu entorno)
ruta_csv = "/home/vboxuser/Tasa_de_Cambio_Representativa_del__Mercado_-Historico.csv"

df_trm = spark.read.csv(ruta_csv, header=True, inferSchema=True)

# 3️⃣ Mostrar esquema y primeras filas
print("=== ESQUEMA DEL DATASET ===")
df_trm.printSchema()

print("=== PRIMERAS FILAS ===")
df_trm.show(5)

# 4️⃣ Limpieza de datos: eliminar nulos y duplicados
df_trm_clean = df_trm.dropna().dropDuplicates()

# 5️⃣ Convertir columnas de fecha si es necesario
df_trm_clean = df_trm_clean.withColumn("anio", year(col("Vigencia_desde")))                            .withColumn("mes", month(col("Vigencia_desde")))

# 6️⃣ Calcular estadísticas por año
df_anual = df_trm_clean.groupBy("anio").agg(
    avg("Valor").alias("TRM_promedio_anual"),
    max("Valor").alias("TRM_max_anual"),
    min("Valor").alias("TRM_min_anual")
).orderBy("anio")

print("=== ESTADÍSTICAS ANUALES ===")
df_anual.show(10)

# 7️⃣ Calcular estadísticas por mes (promedio mensual)
df_mensual = df_trm_clean.groupBy("anio", "mes").agg(
    avg("Valor").alias("TRM_promedio_mensual")
).orderBy("anio", "mes")

print("=== ESTADÍSTICAS MENSUALES ===")
df_mensual.show(12)

# 8️⃣ Guardar los resultados en formato Parquet
df_anual.write.mode("overwrite").parquet("/home/vboxuser/resultados/trm_anual.parquet")
df_mensual.write.mode("overwrite").parquet("/home/vboxuser/resultados/trm_mensual.parquet")

# 9️⃣ Finalizar sesión
spark.stop()
print("✅ Proceso batch finalizado correctamente.")

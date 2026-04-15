# Proyecto Big Data – Tasa de Cambio Representativa del Mercado (TRM)

**Autor:** Anderson David Tapia Ochoa
**Programa:** Ingeniería de Sistemas
**Universidad:** UNAD – Escuela de Ciencias Básicas, Tecnología e Ingeniería (ECBTI)
**Curso:** Procesamiento de Datos con Apache Spark
**Tarea 3:** Análisis y Procesamiento de Datos

---

## Descripción del Proyecto

Este proyecto tiene como objetivo analizar grandes volúmenes de datos históricos correspondientes a la Tasa de Cambio Representativa del Mercado (TRM) en Colombia, utilizando Apache Spark para procesamiento batch y streaming, y Apache Kafka para la simulación de flujo de datos en tiempo real.

El conjunto de datos fue obtenido desde la plataforma oficial de Datos Abiertos de Colombia, en el dataset:
**Tasa de Cambio Representativa del Mercado - Histórico**

---

## Componentes Implementados

| Componente | Descripción |
|---|---|
| Apache Spark (Batch) | Procesa el dataset CSV completo de la TRM, realiza limpieza, conversión de fechas y cálculos anuales/mensuales. |
| Apache Kafka (Streaming) | Simula la llegada de nuevos valores de TRM en tiempo real a través de un productor. |
| Spark Structured Streaming | Consume los mensajes del topic de Kafka, calcula promedios de TRM en ventanas de tiempo y muestra resultados dinámicos. |
| Power BI / Matplotlib | Se utiliza para visualizar las tendencias históricas y resultados obtenidos. |

---

## Archivos del Proyecto

| Archivo | Descripción |
|---|---|
| `spark_trm_batch.py` | Script principal de procesamiento batch de datos históricos. |
| `spark_trm_stream.py` | Script de procesamiento en tiempo real con Apache Spark Streaming. |
| `trm_producer.py` | Simula la generación de datos en tiempo real y los envía al topic Kafka. |

---

## Ejecución del Proyecto

### 1. Procesamiento Batch
```bash
spark-submit spark_trm_batch.py
```

### 2. Procesamiento Streaming con Kafka
Asegúrate de tener Kafka corriendo localmente (`localhost:9092`) y ejecuta:
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_trm_stream.py
```

### 3. Productor de datos Kafka
```bash
python3 trm_producer.py
```

---

## Resultados Esperados

- Promedio, máximo y mínimo anual de la TRM.
- Promedio mensual de los últimos cinco años.
- Visualización de la tendencia anual y mensual.
- Cálculo de promedio de TRM en ventanas de un minuto (streaming).

---

## Arquitectura de la Solución

La arquitectura combina procesamiento batch y streaming, integrando Apache Spark y Kafka.
Los resultados se almacenan en formato Parquet y se visualizan mediante herramientas analíticas.

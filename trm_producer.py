# ============================================
# Archivo: trm_producer.py
# Descripción: Envía datos simulados de la TRM a Kafka
# ============================================

from kafka import KafkaProducer
import json, time, random, datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    msg = {
        "fecha": str(datetime.date.today()),
        "valor": round(random.uniform(3500, 5000), 2)
    }
    producer.send("trm_data", value=msg)
    print(f"📤 Enviado: {msg}")
    time.sleep(2)

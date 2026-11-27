import os
import csv
import json
import time

from pathlib import Path
from kafka import KafkaProducer


bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")
api_key = os.getenv("KAFKA_API_KEY")
api_secret = os.getenv("KAFKA_API_SECRET")


if not bootstrap_server or not api_key or not api_secret:
    raise ValueError("Kafka environment variables are not configured.")


producer = KafkaProducer(
    bootstrap_servers=bootstrap_server,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=api_key,
    sasl_plain_password=api_secret,
    acks="all",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


csv_path = r"C:\Users\user\Documents\PythonVEnv\Kafka-Practice\employee.csv"

with open(csv_path, mode="r", encoding="utf-8-sig") as file:

    reader = csv.DictReader(file)

    for row in reader:

        row["employee_id"] = int(row["employee_id"])
        row["salary"] = int(row["salary"])

        future = producer.send(
            "employee_events",
            value=row
        )

        metadata = future.get(timeout=10)

        print(
            f"Sent: {row} | "
            f"Partition: {metadata.partition} | "
            f"Offset: {metadata.offset}"
        )

        time.sleep(1)


producer.flush()
producer.close()

print("All CSV records sent successfully.")

from kafka import KafkaProducer
import json
import time
import os

# Confluent Cloud configuration
producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVER"],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.environ["KAFKA_API_KEY"],
    sasl_plain_password=os.environ["KAFKA_API_SECRET"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    acks="all"
)

# Employee sample data
employees = [
    {
        "employee_id": 101,
        "name": "Rahul",
        "department": "IT",
        "salary": 50000
    },
    {
        "employee_id": 102,
        "name": "Amit",
        "department": "HR",
        "salary": 40000
    },
    {
        "employee_id": 103,
        "name": "Priya",
        "department": "Finance",
        "salary": 60000
    },
    {
        "employee_id": 104,
        "name": "Neha",
        "department": "IT",
        "salary": 55000
    }
]

try:
    for employee in employees:

        future = producer.send(
            "employee_events",
            value=employee
        )

        metadata = future.get(timeout=30)

        print(
            f"Published: {employee} | "
            f"Partition: {metadata.partition} | "
            f"Offset: {metadata.offset}"
        )

        time.sleep(1)

    producer.flush()

    print("All employee records published successfully!")

finally:
    producer.close()

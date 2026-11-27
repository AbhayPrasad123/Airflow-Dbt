from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

import os



producer_config = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVER"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"]
}



schema_registry_config = {
    "url": os.environ["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": (
        f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:"
        f"{os.environ['SCHEMA_REGISTRY_API_SECRET']}"
    )
}

schema_registry_client = SchemaRegistryClient(
    schema_registry_config
)



schema_str = """
{
    "type": "record",
    "name": "Employee",
    "fields": [
        {"name": "employee_id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "department", "type": "string"},
        {"name": "salary", "type": "int"}
    ]
}
"""


avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    conf={"auto.register.schemas": False}
)

# Create Kafka Producer
producer = Producer(producer_config)


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



def delivery_report(err, msg):

    if err is not None:
        print(f"Delivery failed: {err}")

    else:
        print(
            f"Published: {msg.key().decode()} | "
            f"Partition: {msg.partition()} | "
            f"Offset: {msg.offset()}"
        )


try:

    for employee in employees:

        serialized_value = avro_serializer(
            employee,
            SerializationContext(
                "employee_events",
                MessageField.VALUE
            )
        )

        producer.produce(
            topic="employee_events",
            key=str(employee["employee_id"]).encode("utf-8"),
            value=serialized_value,
            on_delivery=delivery_report
        )

        producer.poll(0)

    producer.flush()

    print("All employee records published successfully!")

except Exception as e:
    print(f"Error: {e}")
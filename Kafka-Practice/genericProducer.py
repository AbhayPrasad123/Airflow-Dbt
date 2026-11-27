import os
import csv
import json

from pathlib import Path
from fastavro import reader

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


# Kafka configuration

producer_config = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVER"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
    "acks": "all"
}

producer = Producer(producer_config)


# Delivery report

def delivery_report(err, msg):

    if err is not None:
        print(f"Delivery failed: {err}")

    else:
        print(
            f"Published | "
            f"Topic: {msg.topic()} | "
            f"Partition: {msg.partition()} | "
            f"Offset: {msg.offset()}"
        )


# CSV reader

def read_csv(file_path):

    with open(file_path, "r", encoding="utf-8-sig") as file:

        for row in csv.DictReader(file):

            yield row


# JSON reader

def read_json(file_path):

    with open(file_path, "r", encoding="utf-8") as file:

        data = json.load(file)

        if isinstance(data, dict):
            data = [data]

        if not isinstance(data, list):
            raise ValueError("Invalid JSON structure")

        for record in data:

            if not isinstance(record, dict):
                raise ValueError("Expected a JSON object")

            yield record


# Schema Registry client

def get_schema_registry_client():

    schema_registry_config = {
        "url": os.environ["SCHEMA_REGISTRY_URL"],
        "basic.auth.user.info": (
            f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:"
            f"{os.environ['SCHEMA_REGISTRY_API_SECRET']}"
        )
    }

    return SchemaRegistryClient(schema_registry_config)


# Avro reader and serializer

def read_avro(file_path, topic):

    schema_registry_client = get_schema_registry_client()

    with open(file_path, "rb") as file:

        avro_reader = reader(file)

        # Extract schema from Avro file

        schema_str = json.dumps(
            avro_reader.writer_schema
        )

        # Create Avro Serializer

        avro_serializer = AvroSerializer(
            schema_registry_client,
            schema_str,
            conf={
                "auto.register.schemas": False
            }
        )

        for record in avro_reader:

            serialized_value = avro_serializer(
                record,
                SerializationContext(
                    topic,
                    MessageField.VALUE
                )
            )

            yield serialized_value


# User input

file_name = input(
    "Enter file name or path: "
).strip().strip('"')

file_format = input(
    "Enter file format (csv/json/avro): "
).strip().lower()

topic = input(
    "Enter Kafka topic: "
).strip()


# File validation

file_path = Path(__file__).parent / file_name

if not file_path.is_file():
    raise FileNotFoundError(
        f"File not found: {file_path}"
    )

if file_format not in ["csv", "json", "avro"]:
    raise ValueError("Unsupported file format")

if file_path.suffix.lower() != f".{file_format}":
    raise ValueError("File format does not match extension")


# Select reader

if file_format == "csv":

    records = read_csv(file_path)

elif file_format == "json":

    records = read_json(file_path)

elif file_format == "avro":

    records = read_avro(file_path, topic)


# Publish records

count = 0

try:

    for record in records:

        if file_format == "avro":

            serialized_value = record

        else:

            serialized_value = json.dumps(
                record
            ).encode("utf-8")

        producer.produce(
            topic=topic,
            value=serialized_value,
            headers={
                "source_format": file_format
            },
            on_delivery=delivery_report
        )

        producer.poll(0)

        count += 1

finally:

    remaining = producer.flush()

    print(f"Total records submitted: {count}")
    print(f"Messages still pending: {remaining}")
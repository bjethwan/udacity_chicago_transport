"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""

    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return
        
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    # Incorrect Rubric: It was said in the course that faust can't handle avro
                    #"key.converter.schemas.registry.url": "http://localhost:8081",
                    "value.converter.schemas.enable": "false",
                    # Incorrect Rubric: It was said in the course that faust can't handle avro
                    #"value.converter.schema.registry.url": "http://localhost:8081",
                    "batch.max.rows": "500",
                    "connection.url": "jdbc:postgresql://postgres:5432/cta",
                    "connection.user": "cta_admin",
                    "connection.password": "chicago",
                    "table.whitelist": "stations",
                    "mode": "incrementing",
                    "incrementing.column.name": "stop_id",
                    "topic.prefix": "org.chicago.cta.",
                    "poll.interval.ms": "60000",
                },
            },
        ),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()

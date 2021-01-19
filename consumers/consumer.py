"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.offset_reset_value = "earliest" if offset_earliest is True else "latest"


        self.broker_properties = {
                "bootstrap.servers": BROKER_URL,
                "schema.registry.url": SCHEMA_REGISTRY_URL,
                "group.id": "org.udacity.public.transportation.consumer"

        }

        if is_avro is True:
            self.consumer = AvroConsumer({
                "bootstrap.servers": self.broker_properties.get("bootstrap.servers"),
                "schema.registry.url": self.broker_properties.get("schema.registry.url"),
                "group.id": self.broker_properties.get("group.id"),
                'auto.offset.reset': self.offset_reset_value
                })
        else:
            self.consumer = Consumer(
                {
                "bootstrap.servers": self.broker_properties.get("bootstrap.servers"),
                "group.id": self.broker_properties.get("group.id"),
                'auto.offset.reset': self.offset_reset_value
                })

        self.consumer.subscribe(
            [topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""

        if self.offset_earliest is True:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.debug("partitions assigned for %s", self.topic_name_pattern)
        logger.debug("consumer on_assign complete!")
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            logger.debug(f"Checking messaage on topic: {self.topic_name_pattern}")
            message = self.consumer.poll(1.0)
            if message is None:
                logger.debug("no message received by consumer")
            elif message.error() is not None:
                logger.debug(f"Error while consuming data: {message.error()}")
            else:
                logger.debug(f"consumed message {message.key()}: {message.value()}")
                self.message_handler(message)
                return 1
        except SerializerError as er:
            logger.error(f"Error while consuming data: {er.message}")
            return 0
            
        return 0
        


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()

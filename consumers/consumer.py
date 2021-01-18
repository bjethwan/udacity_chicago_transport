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

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
                "bootstrap.servers": BROKER_URL,
                "schema.registry.url": SCHEMA_REGISTRY_URL,
                "group.id": "org.udacity.public.transportation.consumer"

        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.consumer = AvroConsumer({
                "bootstrap.servers": self.broker_properties.get("bootstrap.servers"),
                "schema.registry.url": self.broker_properties.get("schema.registry.url"),
                "group.id": self.broker_properties.get("group.id")
                })
        else:
            self.consumer = Consumer(
                {
                "bootstrap.servers": self.broker_properties.get("bootstrap.servers"),
                "group.id": self.broker_properties.get("group.id")
                })

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe(
            [topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        for partition in partitions:
            partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        logger.info("consumer on_assign complete!")
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        logger.info("Infinite consume started")
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            logger.info(f"Checking messaage on topic: {self.topic_name_pattern}")
            message = self.consumer.poll(1.0)
            if message is None:
                print("no message received by consumer")
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                print(f"consumed message {message.key()}: {message.value()}")
                logger.info(f"Received messaage on topic: {message.topic()}")
                self.message_handler(message)
                return 1
        except SerializerError as er:
            logger.error(f"Error while consuming data: {er.message}")
            return 0
            
        return 0
        


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()

"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

BROKER_URLS = "PLAINTEXT://localhost:9092"
SCHEMA_REGISTRY = "http://localhost:8081"

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": BROKER_URLS,
            "schema.registry.url": SCHEMA_REGISTRY,
            # TODO
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        try:
            client = AdminClient({"bootstrap.servers" : BROKER_URLS})
            metadata = client.list_topics(timeout=5)
            if self.topic_name in metadata.topics:
                logger.info(f"{self.topic_name} already exists")
            else:
                futures = client.create_topics([NewTopic(topic=self.topic_name,num_partitions=self.num_partitions,replication_factor=self.num_replicas)])
                for topic, future in futures.items():
                    try:
                        future.result()
                        logger.info(f"topic {self.topic_name} created.")
                    except Exception as e:
                        logger.error(f"topic {self.topic_name} {str(e)} creation failed.")
        except Exception as e:
            logger.info("topic creation kafka integration incomplete - skipping")
            logger.error(f"error = {str(e)}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        try:
            self.producer.flush()
            logger.info("producer closed successfully")
        except Exception as e:
            logger.info("producer close incomplete - skipping")
            logger.error(f"error = {str(e)}")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

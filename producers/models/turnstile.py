"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    value_schema = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        super().__init__(
            topic_name="org.chicago.cta.station.turnstile.v1", 
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=3,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""

        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        msg = f"{num_entries} riders have entered station {self.station.name}" \
              f" at {timestamp.isoformat()}"
        logger.debug(msg)

        for _ in range(num_entries):
            try:
                k = {"timestamp": self.time_millis()}
                v = {
                    "station_id": self.station.station_id,
                    "station_name": self.station.name,
                    "line": self.station.color.name,
                }
                self.producer.produce(topic=self.topic_name,key=k,value=v )
            except Exception as e:
                logger.fatal(e)
                raise

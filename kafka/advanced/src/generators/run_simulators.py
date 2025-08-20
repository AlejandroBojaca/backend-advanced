from .sensor_simulator import SensorSimulator
import asyncio
from kafka import KafkaProducer
import json
import random
import logging


class Runner:
    def __init__(self, num_simulators = 1000):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        self.num_simulators = num_simulators
        self.simulators = []
        self.logger = logging.getLogger(__name__)
   
    def on_send_success(self, record_metadata):
        self.logger.info(f"Message delivered to {record_metadata.topic} "
                f"[{record_metadata.partition}] @ {record_metadata.offset}")

    def on_send_error(self, excp):
        self.logger.error("Failed to send message", exc_info=excp)

    async def create_simulators(self):
        for i in range(self.num_simulators):
            type = random.choice(["temperature", "humidity", "motion"])
            ss = SensorSimulator(type, sensor_id=f"r00-{i}",f = 10 + int(random.random() * 5))
            self.simulators.append(ss)

        print(f"Created {len(self.simulators)} sensor simulators")
        self.logger.info(f"Created {len(self.simulators)} sensor simulators")

    async def run_simulator(self, simulator):
        try:
            async for readings in simulator.generate_readings():
                self.logger.warning(f"Sending data for sensor #{readings['sensor_id']} f={readings['reading_frequency']}.")
                self.producer.send(
                    'readings',
                    value=readings
                ).add_callback(self.on_send_success).add_errback(self.on_send_error)
        
        except Exception as e:
            self.logger.warning(f"Simulator {simulator.sensor_id} failed: {str(e)}")

    async def run_simulators(self):
        try:
            await asyncio.gather(
                *[self.run_simulator(simulator) for simulator in self.simulators]
            )
        except Exception as e:
            self.logger.error(f"Global error: {str(e)}")


# async def a():
#     runner = Runner()
#     await runner.create_simulators()
#     await runner.run_simulators()

# asyncio.run(a())
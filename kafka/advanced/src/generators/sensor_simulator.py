import random
import math
from datetime import datetime
import asyncio


class SensorSimulator:
    def __init__(self, type, e = 10, sensor_id = '0', f = 10):
        self.sensor_id = sensor_id
        self.e = e # constant base humidity
        self.f = f # frequency of readings in seconds
        self.type = type
        self.battery_level = 100

    def _generate_temperature(self):
        time = datetime.now().time()
        hour = time.hour
        hour_temperature = hour if hour <= 12 else 24 - hour
        temperature = 8 + hour_temperature + random.random() * 2 # Base t + random coeficient based on hour + rand 2 degrees

        return temperature

    def _generate_humidity(self):
        temperature = self._generate_temperature()
        saturation_vapor_pressure = 6.112 * math.exp((17.67 * temperature) / (temperature + 243.5))
        return (self.e / saturation_vapor_pressure) * 100

    def _generate_motion(self) -> float:
        now = datetime.now()
        hour = now.hour
        
        if 6 <= hour <= 22:
            probability = 0.3
        else:
            probability = 0.05
            
        return 1.0 if random.random() < probability else 0.0
    
    async def generate_readings(self):
        while True:
            simulate_random_failure = True if random.random() >= 0.98 else False
            if simulate_random_failure:
                raise Exception(f"Sensor {self.sensor_id} failed, Please reset sensor!")
        
            if self.type == "temperature":
                value = self._generate_temperature()
                unit = "°C"
            elif self.type == "humidity":
                value = self._generate_humidity()
                unit = "%"
            elif self.type == "motion":
                value = self._generate_motion()
                unit = "detected"

            self.battery_level = self.battery_level - random.random()
            if self.battery_level <= 0.0:
                raise Exception(f"Sensor's {self.sensor_id} battery finished")

            yield {
                "sensor_id": self.sensor_id,
                "type": self.type,
                "reading": f"{value} {unit}",
                "battery": f"{self.battery_level} %",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "reading_frequency": f"{self.f}s"
            }    

            await asyncio.sleep(self.f)    

# async def b():
#     ss2 = SensorSimulator(sensor_id='U2', f=2)
#     gen2 = ss2.generate_readings()
#     async for x in gen2:
#         print(x)

# async def a():
#     ss = SensorSimulator()
#     gen = ss.generate_readings()
#     async for x in gen:
#         print(x)
    
# async def run_tasks():
#    await asyncio.gather(b(), a())

# asyncio.run(run_tasks())
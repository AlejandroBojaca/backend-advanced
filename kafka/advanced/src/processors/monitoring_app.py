import faust
import redis.asyncio as redis
import json
from datetime import datetime, timedelta


app = faust.App(
    'sensor-readings',
    broker='kafka://localhost:9092',
    value_serializer='raw',
)
readings_topic = app.topic('readings')
alerts_topic = app.topic('device-alerts')
offline_devices = app.topic('offline-devices')

device_states = app.Table('device-states', default=dict)

redis_client = redis.Redis(host="localhost", port=6379)

class AlertManager:
    """Manage alert generation and deduplication"""
    
    async def should_alert(self, device_id: str, alert_type: str, level: int) -> bool:
        """Check if we should send this alert (rate limiting)"""
        key = f"alert:{device_id}:{alert_type}:{level}"
        
        # Check last alert time
        last_alert = await redis_client.get(key)
        if last_alert:
            last_time = datetime.fromisoformat(last_alert.decode())
            
            # Different cooldown periods based on alert level
            cooldown_minutes = {
                "INFO": 30,
                "WARNING": 15,
                "CRITICAL": 5
            }
            
            if datetime.utcnow() - last_time < timedelta(minutes=cooldown_minutes[alert_type]):
                return False
        
        # Record this alert time
        await redis_client.setex(key, 3600, datetime.utcnow().isoformat())  # 1 hour expiry
        return True
    
    async def create_alert(self, device_id: str, alert_type: str, level, 
                          message: str, value: float = None, threshold: float = None):
        """Create and send alert"""
        alert = {
            "device_id":device_id,
            "alert_type":alert_type,
            "level":level,
            "message":message,
            "value":value,
            "threshold":threshold
            }
        
        # Send to alerts topic
        await alerts_topic.send(value=json.dumps(alert).encode('utf-8'))
        
        # Log to console
        print(f"🚨 ALERT {level} {device_id}: {message}")
        
        return alert

alert_manager = AlertManager()


async def process_temperature(temperature, sensor_id, battery_level):
    if temperature < 12:
        should_alert = await alert_manager.should_alert(sensor_id, "WARNING", 7)
        if should_alert:
            await alert_manager.create_alert(sensor_id, "WARNING", 7, "LOW TEMPERATURE", temperature, 12)
    else:
        print(f"TEMPERATURE READING: {temperature} degrees on device {sensor_id}, battery: {battery_level} %")


async def process_humidity(humidity, sensor_id, battery_level):
    if humidity > 70:
        should_alert = await alert_manager.should_alert(sensor_id, "WARNING", 7)
        if should_alert:
            await alert_manager.create_alert(sensor_id, "WARNING", 7, "HIGH HUMIDITY", humidity, 50)
    else:
        print(f"HUMIDITY READING: {humidity} % on device {sensor_id}, battery: {battery_level} %")


async def process_motion(motion, sensor_id, battery_level):
    if motion == 1.0:
        # Motion detected - could implement pattern analysis here
        print(f"👁️  Motion detected on device {sensor_id}, battery: {battery_level} %")

async def process_low_battery(battery_level, sensor_id):
    if battery_level > 5.00:
        should_alert = await alert_manager.should_alert(sensor_id, "WARNING", 7)
        if should_alert:
            await alert_manager.create_alert(sensor_id, "WARNING", 7, "LOW BATTERY", battery_level)
    else:
        should_alert = alert_manager.should_alert(sensor_id, "CRITICAL", 10)
        if should_alert:
            await alert_manager.create_alert(sensor_id, "CRITICAL", 10, "CRITICAL BATTERY LEVEL", battery_level)

async def process_reading_error(sensor_id):
    if await alert_manager.should_alert(sensor_id, "CRITICAL", 10):
        await alert_manager.create_alert(sensor_id, "CRITIAL", 10, "ERROR GETTING READINGS", None)

@app.agent(readings_topic)
async def process_readings(readings):
    async for reading in readings:
        reading = json.loads(reading)
        sensor_id = reading["sensor_id"]
        try:
            value = float(reading["reading"].split(' ')[0])
            battery_level = float(reading["battery"].split(' ')[0])
        except (KeyError, AttributeError, IndexError, ValueError, TypeError) as e:
            print(f"Error processing reading {reading}: {e}")
            await process_reading_error(reading["sensor_id"])
            continue

        device_states[sensor_id] = {
            'last_seen': reading["timestamp"],
            'battery_level': reading["battery"],
            'sensor_type': reading["type"],
            'status': 'active',
        }

        if reading["type"] == "temperature":
            await process_temperature(value, sensor_id, battery_level)
        if reading["type"] == "humidity":
            await process_humidity(value, sensor_id, battery_level)
        if reading["type"] == "motion":
            await process_motion(value, sensor_id, battery_level)

        if battery_level < 30.00:
            await process_low_battery(battery_level, sensor_id)


@app.timer(interval=30.0)
async def check_offline_devices():
    """Check for devices that haven't reported in a while"""
    current_time = datetime.now()

    for sensor_id, state in device_states.items():
        if state.get('status') != 'offline':
            last_seen = datetime.fromisoformat(state['last_seen'])
            offline_duration = current_time - last_seen

            if offline_duration > timedelta(minutes=2) and await alert_manager.should_alert(sensor_id, "CRITICAL", 10):
                await alert_manager.create_alert(sensor_id, "CRITIAL", 10, "DEVICE OFFLINE", None)
                await offline_devices.send(value=sensor_id)
                state['status'] = 'offline'

            


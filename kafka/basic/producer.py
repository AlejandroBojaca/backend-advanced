from kafka import KafkaProducer
import json


def main():
    producer = KafkaProducer(bootstrap_servers='localhost:29092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for _ in range(10):
        producer.send('my_favorite_topic', {"foo": "foo"})

if __name__ == "__main__":
    main()  
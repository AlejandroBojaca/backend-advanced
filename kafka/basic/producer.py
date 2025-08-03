from kafka import KafkaProducer
import json

def main():
    producer = KafkaProducer(bootstrap_servers='localhost:2181', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for _ in range(10):
        producer.send('my_favorite_topic', {'foo': 'bar'})

    # Block until a single message is sent (or timeout)
    # future = producer.send('foobar', b'another_message')
    # result = future.get(timeout=60)




if __name__ == "__main__":
    print('here')
    main()  
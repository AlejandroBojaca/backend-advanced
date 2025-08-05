from kafka import KafkaConsumer, TopicPartition
import json


def main():
    consumer = KafkaConsumer(bootstrap_servers='localhost:29092')
    consumer.subscribe(['my_favorite_topic'])

    for msg in consumer:
        print(msg.value)
        print(json.loads(msg.value)['foo'])

if __name__ == "__main__":
    main()
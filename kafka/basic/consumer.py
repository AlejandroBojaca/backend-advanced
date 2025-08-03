from kafka import KafkaConsumer, TopicPartition

def main():
    consumer = KafkaConsumer('my_favorite_topic')
    for msg in consumer:
        print (msg)

    # join a consumer group for dynamic partition assignment and offset commits
    consumer = KafkaConsumer('my_favorite_topic', group_id='my_favorite_group')
    for msg in consumer:
        print (msg)

    # manually assign the partition list for the consumer
    # consumer = KafkaConsumer(bootstrap_servers='localhost:1234')
    # consumer.assign([TopicPartition('foobar', 2)])

if __name__ == "__main__":
    print('here')
    main()
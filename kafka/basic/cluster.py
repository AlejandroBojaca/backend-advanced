from kafka.cluster import ClusterMetadata

clusterMetadata = ClusterMetadata(bootstrap_servers=['broker1:1234'])

# get all brokers metadata
print(clusterMetadata.brokers())

# get specific broker metadata
print(clusterMetadata.broker_metadata('bootstrap-0'))

# get all partitions of a topic
print(clusterMetadata.partitions_for_topic("topic"))

# list topics
print(clusterMetadata.topics())

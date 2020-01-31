# Kafka-Client

This is a collection of clients written to produce and consume messages to and from Kafka broker (or Microsoft Azure EvenHub).

## Installing and Running

One of the dependency is [node-rdkafka](https://github.com/Blizzard/node-rdkafka), take a look at its [README](https://github.com/Blizzard/node-rdkafka/blob/master/README.md) for details on installation.

```
npm install
```

To run producer use any of these:
```
node event_hub/producer.js
node kafka/producer.js
```

To run consumer use any of these:
```
node event_hub/consumer.js
node kafka/consumer.js
```


## Useful links:

* [Kafka-Client](https://kafka.apache.org/)
* [A Practical Introduction to Kafka Storage Internals](https://medium.com/@durgaswaroop/a-practical-introduction-to-kafka-storage-internals-d5b544f6925f)
* [Use Azure Event Hubs from Apache Kafka applications](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview)
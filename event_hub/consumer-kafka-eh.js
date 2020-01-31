/**
 * A program to consume Azure Event Hub messages using Kafka Consumer Group.
 * Consumer offset is commited periodically and persisted by Event Hub
 */

require('dotenv').config();
const Kafka = require('node-rdkafka');

var consumer = new Kafka.KafkaConsumer({
    'group.id' : "P02",
    'metadata.broker.list': process.env.METADATA_BROKER_LIST,
    'security.protocol': process.env.SECURITY_PROTOCOL,
    'sasl.mechanisms': process.env.SASL_MECHANISMS,
    'sasl.username': process.env.SASL_USERNAME,
    'sasl.password': process.env.SASL_PASSWORD,
    'enable.auto.commit': true
    // 'offset_commit_cb': function(err, topicPartitions) {

    //     if (err) {
    //       // There was an error committing
    //       console.error(err);
    //     } else {
    //       // Commit went through. Let's log the topic partitions
    //       console.log(topicPartitions);
    //     }
    
    //   }
  },
  {'auto.offset.reset' : 'beginning'});

const kafkaTopic = process.env.KAFKA_TOPIC;

consumer.on('ready', function() {
    // consumer.queryWatermarkOffsets(kafkaTopic, 0, 5000, function(err, offsets) {
    //     console.log(offsets);
    // });
    console.log('Consumer ready');
    consumer.subscribe([kafkaTopic]);
    consumer.consume();
  });

consumer.on('data', function(data) {
    // Output the actual message contents
    console.log(`${data.partition} - ${data.offset} - ${data.key} -- ${data.value.toString()}`);
});

consumer.on('event.log', function(log) {
    console.log(log);
});

consumer.on('event.error', function(err) {  
    console.error('Error from consumer');
    console.error(err);
});

consumer.connect();

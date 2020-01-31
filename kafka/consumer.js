/**
 * Kafka Consumer Client without Consumer Group using kafka-node library
 */

require('dotenv').config();
const kafka = require('kafka-node');

const   KafkaClient = kafka.KafkaClient,
        Consumer = kafka.Consumer;

const client = new KafkaClient();

var kafkaTopic = process.env.KAFKA_TOPIC;

var payloads = [
    //{topic: kafkaTopic, offset: 0, partition: 0},
    {topic: kafkaTopic, offset: 0, partition: 1},
    //{topic: kafkaTopic, offset: 0, partition: 2}
]

var options = {
    autoCommit: true,
    autoCommitIntervalMs: 100,
}

const consumer = new Consumer(client, payloads, options);

const readMessage = (message) => {
    console.log(`${message.partition} -- ${message.offset} -- ${message.value}`);
    // consumer.commit(function(err, data) {
    //     if(err)
    //         console.log(err);
    //     console.log(data);
    // });
}

consumer.on('message', readMessage)  ;
consumer.on('error', (err) => {
    console.log(err);
});




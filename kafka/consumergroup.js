/**
 * Kafka Consumer Client with Consumer Group using kafka-node library
 */

require('dotenv').config();
var async = require('async');
const kafka = require('kafka-node');
const ConsumerGroup = kafka.ConsumerGroup;
const KafkaClient = kafka.KafkaClient;

//const client = new KafkaClient();
//const admin = new kafka.Admin(client); // client must be KafkaClient
// admin.listGroups((err, res) => {
//   console.log('consumerGroups', res);
// });

// admin.describeGroups(['test'], (err, res) => {
//   console.log(JSON.stringify(res,null,1));
// })

var consumerOptions = {
  kafkaHost: '127.0.0.1:9092',
  groupId: 'test',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  autoCommit: false,
  fromOffset: 'latest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};

var topics = [process.env.KAFKA_TOPIC];

var consumerGroup = new ConsumerGroup(consumerOptions, topics);
consumerGroup.on('error', onError);
consumerGroup.on('message', onMessage);

function onError (error) {
  console.error(error);
  console.error(error.stack);
}

function onMessage (message) {
  console.log(`${message.partition} -- ${message.offset} -- ${message.value}`);
  consumerGroup.commit((err, data) => {
    if(err)
      console.log("Error: " + err)
    //console.log(data);
  })
}

process.once('SIGINT', function () {
  consumerGroup.close(true, (err) => {
    if(err)
      console.log(err);
  });
});
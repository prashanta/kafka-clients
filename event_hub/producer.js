/* Copyright (c) Microsoft Corporation. All rights reserved.
 * Copyright (c) 2016 Blizzard Entertainment
 * Licensed under the MIT License.
 *
 * Original Blizzard node-rdkafka sample modified for use with Azure Event Hubs for Apache Kafka Ecosystems
 */

require('dotenv').config();
var Kafka = require('node-rdkafka');

var producer = new Kafka.Producer({
  //'debug' : 'all',
  'metadata.broker.list': process.env.metadata_broker_list,
  'dr_cb': true,  //delivery report callback
  'security.protocol': process.env.security_protocol,
  'sasl.mechanisms': process.env.sasl_mechanism,
  'sasl.username': process.env.sasl_username,
  'sasl.password': process.env.sasl_password
});

var kafkaTopic = process.env.KAFKA_TOPIC;

//logging debug messages, if debug is enabled
producer.on('event.log', function(log) {
  console.log(log);
});

//logging all errors
producer.on('event.error', function(err) {
  console.error('Error from producer');
  console.error(err);
});

//counter to stop this sample after maxMessages are sent
var counter = 0;
var maxMessages = 10;

producer.on('delivery-report', function(err, report) {
  console.log('delivery-report: ' + JSON.stringify(report));
  counter++;
});

//Wait for the ready event before producing
producer.on('ready', function(arg) {
  console.log('producer ready.' + JSON.stringify(arg));

  for (var i = 0; i < maxMessages; i++) {
    var value = Buffer.from(`{"name" : "person${i}"}"`);
    var key = "key-"+i;
    // if partition is set to -1, librdkafka will use the default partitioner
    var partition = -1;
    producer.produce(kafkaTopic, partition, value, key);
  }

  //need to keep polling for a while to ensure the delivery reports are received
  var pollLoop = setInterval(function() {
      producer.poll();
      if (counter === maxMessages) {
        clearInterval(pollLoop);
        producer.disconnect();
      }
    }, 1000);

});

producer.on('disconnected', function(arg) {
  console.log('producer disconnected. ' + JSON.stringify(arg));
});

//starting the producer
producer.connect();
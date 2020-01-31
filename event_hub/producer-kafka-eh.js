require('dotenv').config();
var Kafka = require('node-rdkafka');

var producer = new Kafka.Producer({
  'metadata.broker.list': process.env.metadata_broker_list,
  'security.protocol': process.env.security_protocol,
  'sasl.mechanisms': process.env.sasl_mechanisms,
  'sasl.username': process.env.sasl_username,
  'sasl.password': process.env.sasl_password,
  'dr_cb': true  //delivery report callback
});


console.log('Kafka Producer starting... ');

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


producer.on('delivery-report', function(err, report) {
  console.log(`Partition: ${report.partition} - offset: ${report.offset} - key: ${report.key.toString()}`);
});

//Wait for the ready event before producing
producer.on('ready', function(arg) {
  console.log('producer ready.' + JSON.stringify(arg));
  sendMessage();
});
  
let i = 1;

const sendMessage = () => {
    setInterval(()=>{
        const value = Buffer.from(`{"timestamp": ${Date.now()}, "metric": 'temperature', "value": ${i}}`);
        const partition = -1;
        const key = Math.floor(Math.random() * 15);
        producer.produce(kafkaTopic, partition, value, key);
        i++;
        producer.poll(); // In order to get the events in librdkafka's queue to emit, you must call this regularly.
    },100);
}


producer.on('disconnected', function(arg) {
  console.log('producer disconnected. ' + JSON.stringify(arg));
});

//starting the producer
producer.connect();

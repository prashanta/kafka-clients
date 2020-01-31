/**
 * Kafka Producer using 'node-rdkafka' library.
 * 
 * This will cause 'LibrdKafkaError: Local: Queue full' error to be produced 2 times (for the last two messages unable to send).
 * 
 * Decrease maxMessage or increase queue.buffering.max.messages to avoid this error.
 * 
 **/

require('dotenv').config();
var Kafka = require('node-rdkafka');

var counter = 0;
var maxMessages = 104;
var bufferMaxMessages = 110;
var bufferMaxSizeKB = 1; // for 102 messages at 10 byters/message

var producer = new Kafka.Producer({
  //'debug' : 'all',
  'metadata.broker.list': 'localhost:9092',
  'security.protocol': 'plaintext', // default value
  'sasl.mechanisms': 'GSSAPI', // default value,
  'queue.buffering.max.messages' : bufferMaxMessages,  // Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions.
  'queue.buffering.max.kbytes': bufferMaxSizeKB,       // Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions. This property has higher priority than queue.buffering.max.messages.  
  'dr_cb': true  //delivery report callback
});

var kafkaTopic = process.env.KAFKA_TOPIC;

//logging debug messages, if debug is enabled
producer.on('event.log', function(log) {
  console.error('event.log occured');
  console.log(log);
});

//logging all errors
producer.on('event.error', function(err) {
  console.error('event.error occured');
  console.error(err);
});

producer.on('delivery-report', function(err, report) {
  if(report) {
    console.log(`delivery-report: T: ${report.topic} \t P: ${report.partition} \t O: ${report.offset} \t K: ${report.key.toString()} \t T: ${( new Date(report.timestamp)).toISOString()} \t Size: ${report.size} Bytes`);
    counter++;
  }
});

//Wait for the ready event before producing
producer.on('ready', function(arg) {
  console.log('producer ready.' + JSON.stringify(arg));
  // producer.setPollInterval(1);
  for (var i = 0; i < maxMessages; i++) {
    var value = Buffer.alloc(10, 'b'); // 10 bytes of data
    var key = "key-"+i;
    
    var partition = -1;
    var headers = [
      { header: "header value" }
    ]
    try{
      producer.produce(kafkaTopic, partition, value, key, Date.now(), "", headers);
    } 
    catch(err) {
      console.error(err);
      // Asking producer to poll will cause the last message to be sent 
      // producer.poll(); // this is blocking so won't go to next iteration until polling is done.
    }
  }

  //need to keep polling for a while to ensure the delivery reports are received
  var pollLoop = setInterval(function() {
      console.log('polling');
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
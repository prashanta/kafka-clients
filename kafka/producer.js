/**
 * Kafka Producer using 'kafka-node' library.
 * 
 **/

const kafka = require('kafka-node');

const   KafkaClient = kafka.KafkaClient,
        Producer = kafka.Producer,
        KeyedMessage = kafka.KeyedMessage;

const kafkaTopic = process.env.KAFKA_TOPIC;

const client = new KafkaClient(clientOptions);

const producer = new Producer(client, {requireAcks:1, partitionerType: 2});

km = new KeyedMessage('key', 'howedy');
let i = 0;
const sendMessage = () => {
    setInterval(()=>{
        const payload = {timestamp: Date.now(), metric: 'temperature', value: i};
        producer.send([{topic: kafkaTopic, messages: JSON.stringify(payload)}], (err, data) => {
            if(err)
                console.log(err);
            console.log(data);
        });    
        i++;
    },5000);
}

producer.on('ready', sendMessage)  ;
producer.on('error', (err) => {
    console.log(err);
});




/**
 * EventHub Consumer Client using Azure Event-Hubs ibrary
 */

require('dotenv').config();
const { EventHubClient, EventPosition, delay } = require("@azure/event-hubs");

// Connection string - primary key of the Event Hubs namespace. 
const connectionString = process.env.SASL_PASSWORD;

// Name of the event hub. For example: myeventhub
const eventHubsName = process.env.KAFKA_TOPIC;

async function main() {

  const client = EventHubClient.createFromConnectionString(connectionString, eventHubsName);
  
  const allPartitionIds = await client.getPartitionIds();
  const firstPartitionId = allPartitionIds[0];

  const receiveHandler = client.receive(
      firstPartitionId, 
      eventData => {
        console.log(`Received message: ${eventData.body} from partition ${firstPartitionId}`);
    }, 
    error => {
        console.log('Error when receiving message: ', error)
    }, 
    {eventPosition: EventPosition.fromEnd()}
    );

  // Sleep for a while before stopping the receive operation.
  //await delay(15000);
  //await receiveHandler.stop();

  //await client.close();
}

main().catch(err => {
  console.log("Error occurred: ", err);
});

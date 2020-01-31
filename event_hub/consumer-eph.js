/**
 * EventHub Consumer Client using Azure Event-Hubs ibrary. Uses EPH - Event Processor Hub.
 */

require('dotenv').config();
const { EventProcessorHost, delay } = require("@azure/event-processor-host");

// Connection string - primary key of the Event Hubs namespace. 
// For example: Endpoint=sb://myeventhubns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
const eventHubConnectionString = process.env.SASL_PASSWORD;

// Name of the event hub. For example: myeventhub
const eventHubName = process.env.KAFKA_TOPIC;

// Azure Storage connection string
const storageConnectionString = process.env.STORAGE_CONNECTION_STRING;

const hostName = "my-eph-3";
const storageContainerName = "scweathertor";

async function main() {
  const eph = EventProcessorHost.createFromConnectionString(
    hostName,
    storageConnectionString,
    storageContainerName,
    eventHubConnectionString,
    {
      eventHubPath: eventHubName,
      onEphError: (error) => {
        console.log("[%s] Error: %O", error);
      }
    }
  );

  eph.start((context, eventData) => {
    console.log(`Received message: ${JSON.stringify(eventData.body)}. from partition ${context.partitionId}`);
    //console.log(context);
  }, error => {
    console.log('Error when receiving message: ', error)
  });

//   // Sleep for a while before stopping the receive operation.
 // await delay(15000);
  //await eph.stop();
}

main().catch(err => {
  console.log("Error occurred: ", err);
});
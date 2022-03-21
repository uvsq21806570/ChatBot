import json

from azure.eventhub import EventHubProducerClient, EventData

CONN_STR = "Endpoint=sb://chatbotnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=9J5konEMaFhjffnsZfNhi8ilMPwXWRI/tfcEUIh0JxM="

# Create a producer client to produce and publish events to the event hub.
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONN_STR, eventhub_name="myeventhub"
)

# Create a batch. You will add events to the batch later.
event_data_batch = (producer.create_batch())
with open("database.json", "r") as file:
    data = json.load(file)
    for i in range(0, len(data)):
        tuple = json.dumps(data[i])  # Convert each dictionnaty iteration into a JSON string.
        event_data_batch.add(EventData(tuple))  # Add event data to the batch.
producer.send_batch(event_data_batch)  # Send the batch of events to the event hub.

# Close the producer.
producer.close()

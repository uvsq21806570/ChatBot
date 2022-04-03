import asyncio
import time
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from data_collect import collect_data, location_from_coordinates

CONN_STR = "Endpoint=sb://chatbotnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=9J5konEMaFhjffnsZfNhi8ilMPwXWRI/tfcEUIh0JxM="


def json_tuple(dataset, location, i):
    return (
        ("{'loc': \"" + location + '", ')
        + (str((dataset[i])["main"]) + str((dataset[i])["components"]))
        .replace("{", "")
        .replace("}", ", ", 2)
        + ("'dt': \"" + str((dataset[i])["dt"]) + '"}')
    ).replace("'", '"')


async def send_data(connect_str=CONN_STR, eventhub="myeventhub"):
    # Create a producer client to produce and publish events to the event hub.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connect_str, eventhub_name=eventhub
    )

    collected_data = collect_data()
    async with producer:
        # Creating a batch.
        event_data_batch = await producer.create_batch()

        for location in range(len(collected_data)):
            localized_data = collected_data[location]
            coordonates = localized_data["coord"]
            data = localized_data["list"]
            loc = location_from_coordinates(coordonates)

            for i in range(len(data)):
                tuple = json_tuple(data, loc, i)
                print(tuple)
                event_data_batch.add(EventData(tuple))

        await producer.send_batch(event_data_batch)
        # Send the batch of events to the event hub.


start_time = time.time()
asyncio.run(send_data())
print("Send messages in {} seconds.".format(time.time() - start_time))

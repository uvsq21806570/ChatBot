import asyncio
import time
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from data_collect import (
    collect_pollution_data,
    collect_recent_data,
    location_from_coordinates,
)

CONN_STR = "Endpoint=sb://chatbotnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=9J5konEMaFhjffnsZfNhi8ilMPwXWRI/tfcEUIh0JxM="


def json_tuple(dataset, location, i):
    return (
        ("{'loc': \"" + location + '", ')
        + (str((dataset[i])["main"]) + str((dataset[i])["components"]))
        .replace("{", "")
        .replace("}", ", ", 2)
        + ("'dt': \"" + str((dataset[i])["dt"]) + '"}')
    ).replace("'", '"')


async def fill_batch(producer, pollution_data):
    # Creating a batch.
    event_data_batch = await producer.create_batch()
    coordonates = pollution_data["coord"]
    loc_data = pollution_data["list"]
    loc = location_from_coordinates(coordonates)

    for i in range(len(loc_data)):
        data = json_tuple(loc_data, loc, i)
        print(data)
        can_add = True
        while can_add:
            try:
                event_data_batch.add(EventData(data))
            except ValueError:
                can_add = False  # EventDataBatch object reaches max_size.

    return event_data_batch


async def send_data(pollution_data, connect_str=CONN_STR, eventhub="lastdata_hub"):
    # Create a producer client to produce and publish events to the event hub.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connect_str, eventhub_name=eventhub
    )

    async with producer:
        event_data_batch = await fill_batch(producer, pollution_data)
        await producer.send_batch(event_data_batch)
        # Send the batch of events to the event hub.


async def send_all_data(delta, time_update):
    start_time = time.time()
    collected_data = collect_pollution_data(delta)
    ingested_data = {
        "brest1_hub": collected_data[0],
        "marseille1_hub": collected_data[1],
        "versailles1_hub": collected_data[2],
        "brest2_hub": collected_data[3],
        "marseille2_hub": collected_data[4],
        "versailles2_hub": collected_data[5],
        "brest3_hub": collected_data[6],
        "marseille3_hub": collected_data[7],
        "versailles3_hub": collected_data[8],
    }

    for event_hub, data in ingested_data.items():
        await send_data(pollution_data=data, eventhub=str(event_hub))

    print("Send messages in {} seconds.".format(time.time() - start_time))

    while True:
        await asyncio.sleep(time_update)
        recent_data = collect_recent_data(time_update)
        if str((recent_data[0])["list"]) != "[]":
            send_data(collect_recent_data(time_update), eventhub="lastdata_hub")
            # time_update = 3600
        else:
            print("...")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        asyncio.ensure_future(send_all_data(delta=9, time_update=60))
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("Closing Loop")
        loop.close()

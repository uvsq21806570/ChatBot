import asyncio
import time
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from data_collect import (
    collect_pollution_data,
    location_from_coordinates,
)

CONN_STR = "Endpoint=sb://airpollution.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=c/4h7XtTWd894204YobAM7DbFbP9/DRvtlpXZ1qK3Lg="


def json_tuple(dataset, loc, i):
    return (
        ("{'loc': \"" + loc + '", ')
        + (str((dataset[i])["main"]) + str((dataset[i])["components"]))
        .replace("{", "")
        .replace("}", ", ", 2)
        + ("'dt': \"" + str((dataset[i])["dt"]) + '"}')
    ).replace("'", '"')


async def fill_batch(producer, data_list):
    event_data_batch = await producer.create_batch()
    for loc_i in range(len(data_list)):
        loc_data = data_list[loc_i]
        coordonates = loc_data["coord"]
        data = loc_data["list"]
        loc = location_from_coordinates(coordonates)

        for i in range(len(data)):
            tuple = json_tuple(data, loc, i)
            print(tuple)
            event_data_batch.add(EventData(tuple))
    return event_data_batch


async def send_recent_data(connect_str=CONN_STR, eventhub="", update_time=1):
    delta = update_time / 3600 / 24 #delta in days
    while True:
        await asyncio.sleep(update_time)
        recent_data = collect_pollution_data(delta)
        if str((recent_data[0])["list"]) != "[]":
            await send_data(connect_str, eventhub)
        else:
            print("...")


async def send_data(connect_str=CONN_STR, eventhub=""):
    # Create a producer client to produce and publish events to the event hub.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connect_str, eventhub_name=eventhub
    )

    collected_data = collect_pollution_data(delta=7)
    async with producer:
        event_data_batch = await fill_batch(producer, collected_data)
        await producer.send_batch(event_data_batch)
        # Send the batch of events to the event hub.


async def send_all_data(connect_str=CONN_STR, eventhub="", update_time=1):
    start_time = time.time()
    await send_data(connect_str, eventhub)
    print("Send data in {} seconds.".format(time.time() - start_time))
    await send_recent_data(connect_str, eventhub, update_time)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    try:
        asyncio.ensure_future(send_all_data(eventhub="data_hub", update_time=60))
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("Closing Loop")
        loop.close()

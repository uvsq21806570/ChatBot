import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

CONN_STR = "Endpoint=sb://chatbotnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=9J5konEMaFhjffnsZfNhi8ilMPwXWRI/tfcEUIh0JxM="
STORAGE_CONN = "DefaultEndpointsProtocol=https;AccountName=chatbotstoreaccount;AccountKey=I3Id9alOT97fwDYVmjHEJUyejesMoLvleXX06L6YqAeSB6yo7568LyPZKYVYItelJ1CgtBGpWBTL+bjHdfKzVw==;EndpointSuffix=core.windows.net"


async def on_event(partition_context, event):
    # Print the event data.
    print("{}".format(event.body_as_str(encoding="UTF-8")))

    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)


async def receive_data(
    connect_str=CONN_STR,
    eventhub="myeventhub",
    consumer="$Default",
    storage_connection=STORAGE_CONN,
    container="mycontainer",
):
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        storage_connection, container
    )

    # Create a consumer client for the event hub.
    consumer = EventHubConsumerClient.from_connection_string(
        connect_str,
        consumer_group=consumer,
        eventhub_name=eventhub,
        checkpoint_store=checkpoint_store,
    )
    async with consumer:
        # Call the receive method by reading from the beginning of the partition
        await consumer.receive_batch(on_event_batch=on_event, starting_position="-1")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(receive_data())

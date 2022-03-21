import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

CONN_STR = "Endpoint=sb://chatbotnamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=9J5konEMaFhjffnsZfNhi8ilMPwXWRI/tfcEUIh0JxM="
STORAGE_CONN = "DefaultEndpointsProtocol=https;AccountName=chatbotstoreaccount;AccountKey=I3Id9alOT97fwDYVmjHEJUyejesMoLvleXX06L6YqAeSB6yo7568LyPZKYVYItelJ1CgtBGpWBTL+bjHdfKzVw==;EndpointSuffix=core.windows.net"

async def on_event(partition_context, event):
    # Print the event data.
    print(
        'Received the event: "{}" from the partition with ID: "{}"'.format(
            event.body_as_str(encoding="UTF-8"), partition_context.partition_id
        )
    )
    print()

    # Update the checkpoint so that the program doesn't read the events
    # that it has already read when you run it next time.
    await partition_context.update_checkpoint(event)


async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore.from_connection_string(STORAGE_CONN, "mycontainer")

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string(
        CONN_STR,
        consumer_group="$Default",
        eventhub_name="myeventhub",
        checkpoint_store=checkpoint_store
    )
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event, starting_position="-1")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    # Run the main method.
    loop.run_until_complete(main())

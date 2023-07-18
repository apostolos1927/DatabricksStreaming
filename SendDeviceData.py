import asyncio
import random
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import json
import time

EVENT_HUB_CONNECTION_STR = "...."  ##fill in with the connection string from EventHub
EVENT_HUB_NAME = "...."  ##fill in with the EventHub instance name


async def run():
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    countries = ["GREECE", "USA", "UK", "INDIA"]
    counter = 5
    async with producer:
        # Create a batch.
        while counter > 0:
            event_data_batch = await producer.create_batch()
            d = {
                "DeviceID": random.randint(1, 100),
                "DeviceNumber": random.randint(1, 100),
                "DeviceCountry": random.choice(countries),
            }
            # Add events to the batch.
            event_data_batch.add(EventData(json.dumps(d)))

            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)
            time.sleep(3)
            print(d)
            counter = counter - 1


asyncio.run(run())

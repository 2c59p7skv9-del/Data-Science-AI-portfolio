import json
import asyncio
import random
import pandas as pd
from aiokafka import AIOKafkaProducer
from faker import Faker
from datetime import datetime

fake = Faker()

TOPIC = "spotify_songs"
BOOTSTRAP_SERVERS = "localhost:29092"
CSV_PATH = "spotify-songs.csv"

def serializer(value):
    return json.dumps(value).encode("utf-8")

songs_df = pd.read_csv(CSV_PATH)
songs_df = songs_df[songs_df["name"].notna()]

names = [fake.name() for _ in range(10)]
names.append("George Boufis")

async def produce():
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serializer
    )

    await producer.start()

    try:
        while True:
            person = random.choice(names)
            song_row = songs_df.sample(1).iloc[0]

            event = {
                "person_name": person,
                "song_name": str(song_row["name"]),
                "listen_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            print("Sending:", event)
            await producer.send_and_wait(TOPIC, event)

            await asyncio.sleep(2)

    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(produce())

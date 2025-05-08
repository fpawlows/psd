import random
import string
import logging
from typing import NamedTuple
import datetime
import uuid


class TemperatureDTO(NamedTuple):
    thermometer_id: int = uuid.uuid4()
    timestamp: datetime.datetime = datetime.datetime.now()
    temperature: float


TOPIC_NAME='Temperatura'

def generate_message1() -> TemperatureDTO:
    # Generate a random message
    temperature_value = float(random.randint(-20000,20000)) / 100
    temp = TemperatureDTO(temperature_value)
    return temp

import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer

# Messages will be serialized as JSON
def serializer(message):
    return json.dumps(message).encode('utf-8')

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=serializer,
)

if __name__ == '__main__':
    # Infinite loop - runs until you kill the program
    while True:
        # Generate a message
        dummy_message_dict = generate_message1()._asdict()

        # Send it to our 'messages' topic
        logging.info(f'Producing message @ {datetime.now()} | Message = {str(dummy_message_dict)}')
        producer.send(TOPIC_NAME, dummy_message_dict)

        # Sleep for a random number of seconds
        time_to_sleep = random.randint(1, 11)
        time.sleep(time_to_sleep)

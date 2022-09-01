from kafka import KafkaProducer
from time import sleep

import json


produc=KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'))


with open("way_coordinates3","r") as coordinates:
    for coordinate in coordinates:
        produc.send("step",json.loads(coordinate))
        sleep(1)

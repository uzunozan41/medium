#medium geopandas2 makalesindeki verileri üreten bot

from kafka import KafkaProducer
import json
from random import randint
from time import sleep

produce=KafkaProducer(value_serializer=lambda m: json.dumps(m).encode("utf-8"))

def produce_step():
    data={}
    data["id"]=randint(1,1100)
    data["step"]=randint(100,200)

    produce.send("step",data)

    return data

while True:
    print(produce_step())
    sleep(0.5)
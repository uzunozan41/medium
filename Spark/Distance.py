from pykafka import KafkaClient
from random import randint,choice
import string
import time
import datetime
import json


client=KafkaClient(hosts="127.0.0.1:9092")

start_topic=client.topics["start-point"]
stop_topic=client.topics["stop-point"]

def create_car_id():
    car_id=str(randint(100,110))
    if randint(1,2)==1:
        car_id=car_id+" "+choice(string.ascii_uppercase)
    else:
        car_id=car_id+" "+choice(string.ascii_uppercase)+choice(string.ascii_uppercase)

    car_id=car_id+" "+str(randint(10,300))

    return car_id

    
cars=[]
for i in range(1,100):
    data1={}
    car_id1=create_car_id()
    cars.append(car_id1)
    data1["car_id"]=car_id1
    data1["time"]=str(datetime.datetime.now())

    start_producer=start_topic.get_producer()
    start_producer.produce(json.dumps(data1).encode("utf-8"))
    print("Data1:"+str(data1))

    
    time.sleep(3)

    if len(cars)>=20:
        car_id2=choice(cars[0:5])
        data2={}
        data2["car_id"]=car_id2
        data2["time"]=str(datetime.datetime.now())
        cars.remove(car_id2)

        stop_prucer=stop_topic.get_producer()
        stop_prucer.produce(json.dumps(data2).encode("utf-8"))

        print("Data2:"+str(data2))
        





    




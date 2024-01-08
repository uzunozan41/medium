import json
import os

from datetime import datetime,timedelta
from random import randint
from hospital_schema import Hospital1,Hospital2,Hospital3
from google.cloud import bigquery
from time import sleep
from google.cloud import pubsub_v1


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/keys/a_beam-**-**.json"

client=bigquery.Client(project="medium-**")

now=datetime.now()

finish_day=datetime(year=now.year,month=now.month,day=now.day,hour=16,minute=0,second=0)
start_lunch=datetime(year=now.year,month=now.month,day=now.day,hour=12,minute=0,second=0)
finish_lunch=datetime(year=now.year,month=now.month,day=now.day,hour=13,minute=0,second=0)


def crate_data(hospital_id,Hospital:dict):
    for key in Hospital.keys():
        first_key=key
        break


    doctor=randint(first_key,first_key+len(Hospital)-1)
    if finish_day<Hospital[doctor]["consultation_time"]:
        pass

    elif start_lunch<Hospital[doctor]["consultation_time"]<finish_lunch:
        Hospital[doctor]["consultation_time"]=datetime(year=now.year,month=now.month,day=now.day,hour=13,minute=0,second=0)

    else:
        data={}
        data["Hospital_id"]=hospital_id
        data["doctor_id"]=doctor
        data["patient_id"]=randint(10001,19999)
        data["clinic_id"]=Hospital[doctor]["clinic_id"]
        data["clinic_name"]=Hospital[doctor]["clinic_name"]
        data["consultation_time"]=datetime.strftime(Hospital[doctor]["consultation_time"],"%Y-%m-%dT%H:%M:%S")

        if data["clinic_id"]!=1007:
            Hospital[doctor]["consultation_time"]=Hospital[doctor]["consultation_time"]+timedelta(seconds=randint(600,1200))
        else:
            Hospital[doctor]["consultation_time"]=Hospital[doctor]["consultation_time"]+timedelta(seconds=randint(300,900))


        row=data
    
    return row

j=0
hospital_list=[Hospital1,Hospital2,Hospital3]
hospital_name=["Hospital1","Hospital2","Hospital3"]

publiser=pubsub_v1.PublisherClient()
topic="projects/medium-**/topics/hospital_topic"

for i in range(0,100):
    hospital_id=randint(0,2)

    data=crate_data(hospital_name[hospital_id],hospital_list[hospital_id])
    j=j+1

    publiser.publish(topic,json.dumps(data).encode("utf-8"))
    print(data)
    sleep(1)

print(j)

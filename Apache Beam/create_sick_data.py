from datetime import datetime,timedelta
from random import randint
from hospital_schema import Hospital1
from google.cloud import bigquery

client=bigquery.Client(project="Your Project Id")

now=datetime.now()

finish_day=datetime(year=now.year,month=now.month,day=now.day,hour=16,minute=0,second=0)
start_lunch=datetime(year=now.year,month=now.month,day=now.day,hour=12,minute=0,second=0)
finish_lunch=datetime(year=now.year,month=now.month,day=now.day,hour=13,minute=0,second=0)


j=1

for i in range(1,randint(900,1200)):
    doctor=randint(101,132)
    if finish_day<Hospital1[doctor]["consultation_time"]:
        pass

    elif start_lunch<Hospital1[doctor]["consultation_time"]<finish_lunch:
        Hospital1[doctor]["consultation_time"]=datetime(year=now.year,month=now.month,day=now.day,hour=13,minute=0,second=0)

    else:
        data={}
        data["doctor_id"]=doctor
        data["patient_id"]=randint(10001,19999)
        data["clinic_id"]=Hospital1[doctor]["clinic_id"]
        data["clinic_name"]=Hospital1[doctor]["clinic_name"]
        data["consultation_time"]=datetime.strftime(Hospital1[doctor]["consultation_time"],"%Y-%m-%dT%H:%M:%S")

        if data["clinic_id"]!=1007:
            Hospital1[doctor]["consultation_time"]=Hospital1[doctor]["consultation_time"]+timedelta(seconds=randint(600,1200))
        else:
            Hospital1[doctor]["consultation_time"]=Hospital1[doctor]["consultation_time"]+timedelta(seconds=randint(300,900))

        j=j+1

        row=[data]

        client.insert_rows_json(table="project-id.medical_system.hospital1_log",json_rows=row)


        print(data)
print(j)

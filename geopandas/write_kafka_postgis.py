import psycopg2
import json

from kafka import KafkaConsumer
from datetime import datetime

#kafka consumer oluşturulması.
consumer=KafkaConsumer('step',bootstrap_servers='127.0.0.1:9092')


#database bağlantı bilgileri
host='127.0.0.1'
user='postgres'
password='1'
port='5432'
database='college'

#sql bağlantı connection ve cursor nesnesinin oluşturulması
connect=psycopg2.connect(database=database,user=user,password=password,port=port,host=host)
cursor=connect.cursor()


#Kafkadan gelen verileri alıyoruz.
for data in consumer:
    #json formatını bir sözlüğe dönüştürüyoruz.
    value=json.loads(data.value)

    #Sql insert sorgumuzu oluşturuyoruz.
    sql="insert into step_log (member_id,step,time) values ({},{},'{}')"\
        .format(value["id"],value["step"],datetime.now())

    #Sql sorgusunu uyguluyoruz.
    cursor.execute(sql)

    #Uyguladığımız değişiklikleri veri tabanına işliyorum.
    connect.commit()




from random import randint
import pymongo
import os

#Burada kendi atlas hesabımı gizlemek için url'i başka bir dosyadan çektim.
#Sizde atlas_url değişkeni yerine kendi atlas hesabınızın url'inizi veya başka bir mongodb url'sini kullana bilirsiniz.
from mongodb_acount import atlas_url

client=pymongo.MongoClient(atlas_url)
database=client["41universe"]
collection=database["gamers"]

title=["dark","strong","fearless","cool","bad","good","killer"]
name=["lord","king","queen","soldier","knight","lady","boy","sir","general","captain"]
items=["sword","bow","staff","crossbow"]


def random_insert():
    gamer = {"level": 1, "score": 1}
    gamer["nickname"] = title[randint(0, 6)] + name[randint(0, 9)] + str(randint(10, 99))
    item = {}
    item[items[randint(0, 3)]] = 1
    gamer["item"] = item

    return gamer

insert=0
update=0
delete=0


nicknames = []
for i in range(1, 50):

    if i<11:
        data = random_insert()
        nicknames.append(data["nickname"])

        collection.insert_one(data)

        insert=insert+1


    else:
        x = randint(1, 4)

        if x == 1:
            quary = {}
            quary["nickname"] = nicknames[randint(0, 9)]
            newvalue = {"$set": {"score": randint(1, 20)}}

            collection.update_one(quary, newvalue)

            update=update+1
        elif x == 2:
            quary = {}
            quary["nickname"] = nicknames[randint(0, 9)]

            collection.delete_one(quary)

            delete=delete+1
        else:
            data = random_insert()
            nicknames.append(data["nickname"])

            collection.insert_one(data)

            insert=insert+1

    os.system("clear")
    print("{} data inserted...".format(insert))
    print("{} data updated....".format(update))
    print("{} data deleted....".format(delete))



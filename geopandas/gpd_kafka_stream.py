import matplotlib.pyplot as plt
import geopandas as gpd
import rasterio
import json


from kafka import KafkaConsumer
from shapely.geometry import Point,LineString
from rasterio.plot import show
from datetime import datetime


consumer=KafkaConsumer('step',bootstrap_servers='127.0.0.1:9092')
fig, ax = plt.subplots(figsize=(15,20))

src=rasterio.open("/home/ozan/Desktop/Qgis/kou_modified.tif")
extent=[src.bounds[0],src.bounds[1],src.bounds[2],src.bounds[3]]

coordinate_list=[]

for data in consumer:
  plt.cla()

  ax = rasterio.plot.show(src, extent=extent, ax=ax)

  value=json.loads(data.value)
  coordinate = value["coordinate"]
  present_point=Point(coordinate[0],coordinate[1])

  plt.title("Geopandas III (Kafka-Stream)",size=15)
  plt.xlabel(datetime.now())
  plt.ylabel("https://medium.com/@uzunozan41")

  coordinate_list.append(present_point)

  if len(coordinate_list)>1:
    present_line = LineString(coordinate_list)
    df_line = gpd.GeoDataFrame(geometry=[present_line])
    df_line.plot(ax=ax, linewidth=3)

  df_point=gpd.GeoDataFrame(geometry=[present_point])
  df_point.plot(ax=ax,color="black",marker="*",markersize=75)
  plt.pause(0.1)


plt.show()
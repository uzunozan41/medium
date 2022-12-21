import matplotlib.pyplot as plt
import geopandas as gpd
import rasterio

from rasterio.plot import show
from matplotlib.animation import FuncAnimation
from sqlalchemy import create_engine
from datetime import datetime




url="postgresql://postgres:1@localhost:5432/college"
connect=create_engine(url)

fig, ax = plt.subplots(figsize=(15,20))

ax.set_title("Kocaeli Üniversitesi", size=20)
ax.set_ylabel("Geopandas II(Postgis-kafka)",size=15)

src=rasterio.open("/home/ozan/Desktop/Qgis/kou_modified.tif")
extent=[src.bounds[0],src.bounds[1],src.bounds[2],src.bounds[3]]

ax = rasterio.plot.show(src, extent=extent, ax=ax)

old_row={}

def stream_gis(i):

    sql = """select
                a.id,
                sum(sl.step) total_step,
                geom
            from academy a
            left join member m 
            on m.college_id = a.id
            left join step_log sl 
            on sl.member_id = m.id
            group by a.id"""
    df = gpd.GeoDataFrame.from_postgis(sql, connect)

    df["label_xy"] = df["geom"].apply(lambda x: x.representative_point().coords[0])

    x_label = "last updated : {} \n https://medium.com/@uzunozan41".format(datetime.now(),size=15)
    ax.set_xlabel(x_label)




    for j,row in df.iterrows():
        id = row["id"]

        if row["id"] not in old_row.keys():

            old_row[id]=ax.annotate(text=row["total_step"], xy=row["label_xy"],
                              horizontalalignment="center", color="black", size=10)
        elif old_row[id].get_text!=row["total_step"]:
            old_row[id].remove()
            old_row[id] = ax.annotate(text=row["total_step"], xy=row["label_xy"],
                                      horizontalalignment="center", color="black", size=10)





    return df.plot(ax=ax,color="lightskyblue")


anim=FuncAnimation(fig,stream_gis,interval=1000)


plt.show()
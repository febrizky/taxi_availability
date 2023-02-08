import psycopg2
import requests
import json
import pandas as pd
from sqlalchemy import create_engine
import geopandas as gpd
import geopy
from geopy.geocoders import Nominatim
import plotly_express as px

conn_string = 'postgres://postgres:root@localhost:5432/test'
  
db = create_engine(conn_string)
conn = db.connect()

sql_query = pd.read_sql_query ('''
                               with cte as (
select max(create_time) max_create_time from taxi_availability ta 
)
select lat,lon from taxi_availability ta2, cte where ta2.create_time = max_create_time
                               ''', conn)

df_lat_lon = pd.DataFrame(sql_query, columns = ['lat', 'lon'])
# print (df_lat_lon)

px.set_mapbox_access_token("pk.eyJ1Ijoic2hha2Fzb20iLCJhIjoiY2plMWg1NGFpMXZ5NjJxbjhlM2ttN3AwbiJ9.RtGYHmreKiyBfHuElgYq_w")
fig = px.scatter_mapbox(df_lat_lon, lat="lat", lon="lon",  zoom=10)
fig.show()

sql_query2 = pd.read_sql_query ('''
                               with cte as (
select max(create_time) max_create_time from taxi_availability ta 
)
select count(1) as taxi_count, plan_area from taxi_availability ta2, cte where ta2.create_time = max_create_time group by 2
                               ''', conn)

df_taxi_availability = pd.DataFrame(sql_query2, columns = ['taxi_count', 'plan_area'])
# print (df_taxi_availability)

df_taxi_availability_max = df_taxi_availability.sort_values(by = 'taxi_count', ascending = False).head(10) 

fig1 = px.bar(df_taxi_availability_max, x="plan_area", y="taxi_count", labels={'taxi_count':'Taxi Count', 'plan_area':'Plan Area'},
                  title="Taxi Count per Area").update_xaxes(categoryorder='total ascending')
fig1.show()

df_taxi_availability_min = df_taxi_availability.sort_values(by = 'taxi_count', ascending = True).head(10) 

fig2 = px.bar(df_taxi_availability_min, x="plan_area", y="taxi_count", labels={'taxi_count':'Taxi Count', 'plan_area':'Plan Area'},
                  title="Taxi Count per Area").update_xaxes(categoryorder='total ascending')
fig2.show()

sql_query3 = '''
                                with cte as(
 select distinct taxi_count, create_time from taxi_count tc)
 select avg(taxi_count) from cte
                                '''
result = db.execute(sql_query3)
row = result.fetchone()
print('Average taxi avalability across Singapore is :', row['avg'])
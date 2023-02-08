import psycopg2
import requests
import json
import pandas as pd
from sqlalchemy import create_engine
from geopy.geocoders import Nominatim

conn_string = 'postgres://postgres:root@localhost:5432/test'
  
db = create_engine(conn_string)
conn = db.connect()

token = """eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjk4MzIsInVzZXJfaWQiOjk4MzIsImVtYWlsIjoiZmVicml6a3lfcmFtYWRhbmlAaG90bWFpbC5jb20iLCJmb3JldmVyIjpmYWxzZSwiaXNzIjoiaHR0cDpcL1wvb20yLmRmZS5vbmVtYXAuc2dcL2FwaVwvdjJcL3VzZXJcL3Nlc3Npb24iLCJpYXQiOjE2NzU3NzQyOTUsImV4cCI6MTY3NjIwNjI5NSwibmJmIjoxNjc1Nzc0Mjk1LCJqdGkiOiJhNzRmMWMyMWU2ODJlZTY5NDRiNTkzNTEyNTBmODZlNCJ9.RCQmz4AhhEtGtRZdXHhr4JDG3SsIiHvzCUG6U9yisDM"""
planning_area_api_url = 'https://developers.onemap.sg/privateapi/popapi/getAllPlanningarea?token='
planning_area_token = planning_area_api_url+token
plan_area = requests.get(planning_area_token)
print(plan_area)

df = pd.read_json(plan_area.text)
conn = db.connect()
df.to_sql('plan_area', con=conn, if_exists='replace',
          index=False)
conn = psycopg2.connect(conn_string
                        )
# conn.autocommit = True
cursor = conn.cursor()

onemap_api = 'https://api.data.gov.sg/v1/transport/taxi-availability'

taxi_data = requests.get(onemap_api)
taxi_location = json.loads(taxi_data.text)

taxi_loc = []
taxi_loc_len = len(taxi_location['features'][0]['geometry']['coordinates'])
for i in range(taxi_loc_len):
    taxi_loc.append(taxi_location['features'][0]['geometry']['coordinates'][i])
df_len = len(taxi_loc)
for i in range(df_len):
    taxi_loc[i].append(taxi_location['features'][0]['properties']['timestamp'])
    print(taxi_loc[i])
# print(taxi_loc)
locator = Nominatim(user_agent="myGeocoder")
for i in range(df_len):
    lat =taxi_loc[i][1]
    lon =taxi_loc[i][0]
    coordinates = str(lat) + ',' + str(lon)
    location = locator.reverse(coordinates)
    try:
        taxi_loc[i].append(location.raw['address']['suburb'])
        print(taxi_loc[i])
    except:
        print('failed to append')
        print(taxi_loc[i])
# print(taxi_loc)

df1 = pd.DataFrame(taxi_loc,columns =['lon', 'lat','create_time', 'plan_area'])

conn = db.connect()
df1.to_sql('taxi_availability', con=conn, if_exists='append',
          index=False)
conn = psycopg2.connect(conn_string
                        )
# conn.autocommit = True
cursor = conn.cursor()
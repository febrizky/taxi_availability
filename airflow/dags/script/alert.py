import requests
import json
from sqlalchemy import create_engine
import pandas as pd

conn_string = 'postgres://postgres:root@localhost:5432/test'
  
db = create_engine(conn_string)
conn = db.connect()


sql_query = pd.read_sql_query ('''
                               with cte as (
select distinct create_time max_create_time from taxi_availability ta 
)
,cte2 as(
select distinct plan_area from taxi_availability ta2, cte where ta2.create_time = max_create_time
)
select distinct pln_area_n
from plan_area full outer join cte2 on replace(lower(pln_area_n),' ','') = (replace(lower(cte2.plan_area),' ','')) 
where (plan_area is null) and pln_area_n not in ('OTHERS')
                               ''', conn)
plan_area_list = sql_query.values.tolist()
plan_name = ','.join(map(str, plan_area_list))
plan_name = plan_name.replace("[", "" )
plan_name = plan_name.replace("]", "" )
plan_name = plan_name.replace("'", "" )
message = ('Alert! There is no taxi available on {}'.format(plan_name))
print(message)
responses = {}
chat_id = "Chat ID"
api_key = "API Key"

# message = "Hello world!!!"

headers = {'Content-Type': 'application/json',
           'Proxy-Authorization': 'Basic base64'}
data_dict = {'chat_id': chat_id,
             'text': message,
             'parse_mode': 'HTML',
             'disable_notification': True}
data = json.dumps(data_dict)
url = f'https://api.telegram.org/bot{api_key}/sendMessage'
response = requests.post(url,
                         data=data,
                         headers=headers)
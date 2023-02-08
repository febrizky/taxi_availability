from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators import BashOperator

from datetime import timedelta

import util


DAG_ID = util.get_dag_id(__file__)


default_args = {
    'owner': 'Febri',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': task_fail_slack_alert,
    'retries': 0,
    'retry_delay' : timedelta(minutes=5),
    'provide_context': True
}


dag = DAG(
    DAG_ID,
    'taxi_availability_job',
    access_control = {
        'Febri': {'can_dag_read', 'can_dag_edit'}
    },
    default_args=default_args,
    schedule_interval='0 * * * *',
    catchup=False)

get_data = BashOperator(
    task_id='get_data',
    bash_command='python /home/airflow/airflow/dags/scripts/get_data.py',
    dag=dag)

show_data = BashOperator(
    task_id='show_data',
    bash_command='python /home/airflow/airflow/dags/scripts/show_data.py',
    dag=dag)

alert = BashOperator(
    task_id='alert',
    bash_command='python /home/airflow/airflow/dags/scripts/alert.py',
    dag=dag)

get_data >> [show_data, alert]
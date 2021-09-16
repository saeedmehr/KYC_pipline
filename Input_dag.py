from airflow.models import DAG
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python_operator import PythonOperator
import random

import datetime
import json
from faker import Faker
import os


args={
    'owner': 'Saeid',
    'start_date': days_ago(1),
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)

}

dag = DAG(dag_id='Input_dag' , default_args = args, schedule_interval=None )

def run_this_func(**context):
    print('Testing task of pipeline')

def JsonDummy(request_id='1',**kwargs):
    data = {}
    s = datetime.datetime(2022, 1, 20,0,0)
    e = datetime.datetime(2022, 2, 20,0,0)

    fake = Faker()
    timestamp = fake.date_time_between(s,e)
    time = timestamp.strftime("%m/%d/%Y %H:%M")
    request_id, request_timestamp, latitude, longitude, document_photo_brightness_percent, is_photo_in_a_photo_selfie, document_matches_selfie_percent, s3_path_to_selfie_photo, s3_path_to_document_photo = [
        request_id, time, '3', '4', '5', '6', '7', '8', '9']

    data = {'request': [{'request_id': request_id, 'request_timestamp': request_timestamp,
                         'latitude': latitude, 'longitude': longitude,
                         'document_photo_brightness_percent':document_photo_brightness_percent,
                         'is_photo_in_a_photo_selfie': is_photo_in_a_photo_selfie,
                         'document_matches_selfie_percent':document_matches_selfie_percent,
                         's3_path_to_selfie_photo':s3_path_to_selfie_photo,
                         's3_path_to_document_photo':s3_path_to_document_photo}]}

    out = json.dumps(data)
    path = 'request/' + 'year=' + str(timestamp.year) + '/' + 'month=' + str(timestamp.month) + '/' + 'day=' + str(timestamp.day) + '/' + 'hour=' + str(timestamp.hour) + '/'
    if not os.path.exists(path):
        os.makedirs(path)
    with open(path+'request_'+request_id+'.json', 'w') as json_file:
        json.dump(data, json_file)

    print('Hello testing for debuging')
    print(os.getcwd())


with dag:

    run_this_task = PythonOperator(
        task_id ='run_this',
        python_callable = run_this_func,
        provide_context = True
       # retries = 10,
       # retry_delay = timedelta(seconds=1) 
    )

    running_fake_data = PythonOperator(
        task_id ='running_fake_data',
        python_callable = JsonDummy,
        provide_context = True,
        #retries = 10,
        #retry_delay = timedelta(seconds=1) 
    )

 

    run_this_task >> running_fake_data 
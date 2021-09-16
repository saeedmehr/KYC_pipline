from airflow.models import DAG
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.utils.dates import days_ago, timedelta
from airflow.operators.python_operator import PythonOperator 
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskMarker, ExternalTaskSensor
from datetime import datetime
from faker import Faker
import os
import random
import sqlite3
import json

args={
    'owner': 'Saeid',
    'start_date': days_ago(1),
    'retries' : 10,
    'retry_delay' : timedelta(seconds=5)
}

dag = DAG(dag_id='File_process' , default_args = args, schedule_interval= None )


# def print_file_context(**context):
#     hook = FSHook('my_file_system_1')
#     print('we are in the hook')
#     path = os.path.join(hook.get_path(), 'request_1411.json')
#     print(path)
#     with open(path, 'r') as fp:
#         print(fp.read())
#         record = json.load(fp)    
#         context['ti'].xcom_push(key='out1', value=record)


#___________________________________________________________________________________________________________________________________________________________________________
#                                               Reading the JSON file from the adress /tmp with the name of request_1411.json as an example
#___________________________________________________________________________________________________________________________________________________________________________

def ReadJSON(**context):
    hook = FSHook('my_file_system_1')
    print('we are in the hook')
    path = os.path.join(hook.get_path(), 'request_1411.json')
    print(path)
    with open('/tmp/request_1411.json') as jsonFile:
        record = json.load(jsonFile)
        context['ti'].xcom_push(key='out1', value=record)
        jsonFile.close() 


# I was mostly testing some theories of mine, to see if i could call dags with this way 
# def GetJSON(**context):  
#     record = context['ti'].xcom_pull(dag_id='Input_generator_dag',task_ids='t1',key="generator")
#     context['ti'].xcom_push(key="generator", value = record )


#___________________________________________________________________________________________________________________________________________________________________________
#                                                                    Transforming the input data into integer 
#___________________________________________________________________________________________________________________________________________________________________________

def transform(**context):
    #record = ReadJSON()
    record = context['ti'].xcom_pull(key='out1')
    record ={'request_id': int([record['request'][0]['request_id']][0]),
             'request_timestamp': [record['request'][0]['request_timestamp']][0],
             'latitude': int([record['request'][0]['latitude']][0]),
             'longitude': int([record['request'][0]['longitude']][0]),
             'document_photo_brightness_percent': int([record['request'][0]['document_photo_brightness_percent']][0]),
             'is_photo_in_a_photo_selfie': int([record['request'][0]['is_photo_in_a_photo_selfie']][0]),
             'document_matches_selfie_percent': int([record['request'][0]['document_matches_selfie_percent']][0]),
             's3_path_to_selfie_photo': int([record['request'][0]['s3_path_to_selfie_photo']][0]),
             's3_path_to_document_photo': int([record['request'][0]['s3_path_to_document_photo']][0])}
    #print(record)
    #return record 
    context['ti'].xcom_push(key='out2', value=record)       

#___________________________________________________________________________________________________________________________________________________________________________
#                                              Creating the first check. basically the first check will give us a random probability_of_fraud
#___________________________________________________________________________________________________________________________________________________________________________

def CheckFraud(**context):
    #result = transform()
    result = context['ti'].xcom_pull(key='out2')
    prob = random.uniform(0, 1)
    print(result)

    if 'probability_of_fraud' not in result.keys():
        result['probability_of_fraud'] = prob
    else:
        result["probability_of_fraud"].append(prob)
    #return result
    context['ti'].xcom_push(key='out3', value=result) 

#___________________________________________________________________________________________________________________________________________________________________________
#                                              Creating the agent check. I assumed we only have 10 agents and randomly one of them will check the requests
#___________________________________________________________________________________________________________________________________________________________________________

def AgentCheck(**context):
    out = None
    #input = CheckFraud()
    input = context['ti'].xcom_pull(key='out3')
    prob = random.uniform(0, 1)
    agent_number = random.randint(1,10)

    if prob > input['probability_of_fraud']:
        out = 1
    else:
        out = 0
    if 'final_decision_by_agent' not in input.keys():
        input['final_decision_by_agent'] = out
    else:
        input["final_decision_by_agent"].append(out)

    if 'agent_id' not in input.keys():
        input['agent_id'] = agent_number
    else:
        input["agent_id"].append(agent_number)
    #return input
    context['ti'].xcom_push(key='out4', value=input)

#___________________________________________________________________________________________________________________________________________________________________________
#                                                                            Creating the SQLite database
#___________________________________________________________________________________________________________________________________________________________________________

def CreateDB():
    try:
        sqliteConnection = sqlite3.connect('SQLite_Python.db')
        sqlite_create_table_query = '''CREATE TABLE Fraud_Data (
                                    request_id INTEGER,
                                    agent_id INTEGER NOT NULL,
                                    probability_of_fraud REAL NOT NULL,
                                    final_decision_by_agent INTEGER ,
                                    insert_timestamp DATETIME);'''

        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")
        cursor.execute(sqlite_create_table_query)
        sqliteConnection.commit()
        print("SQLite table created")

        cursor.close()

    except sqlite3.Error as error:
        print("Error while creating a sqlite table", error)
        sqliteConnection.close()

    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("sqlite connection is closed")

#___________________________________________________________________________________________________________________________________________________________________________
#                                                                            Filling the database with the final output
#___________________________________________________________________________________________________________________________________________________________________________

def FillDB(**context):

    #input = AgentCheck()
    input = context['ti'].xcom_pull(key='out4')
    req_time = datetime.strptime(input['request_timestamp'], '%m/%d/%Y %H:%M')
    try:
        sqliteConnection = sqlite3.connect('SQLite_Python.db')
        sqlite_create_table_query = "INSERT INTO Fraud_Data(request_id,agent_id,probability_of_fraud,final_decision_by_agent) VALUES("+ str(input['request_id'])+","+str(input['agent_id'])+","+str(input['probability_of_fraud'])+","+str(input['final_decision_by_agent'])+")"#+","+req_time+")"

        cursor = sqliteConnection.cursor()
        print("Successfully Connected to SQLite")
        cursor.execute(sqlite_create_table_query)
        sqliteConnection.commit()
        print("test")

        cursor.execute("select * from Fraud_Data")
        rows = cursor.fetchall()
        for row in rows: print(row)

        cursor.close()

    except sqlite3.Error as error:
        print(error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("sqlite connection is closed")

#___________________________________________________________________________________________________________________________________________________________________________
#                                                                            Creating the final decision as a JSON file
#___________________________________________________________________________________________________________________________________________________________________________

def FinalJSON(**context):
    data = context['ti'].xcom_pull(key='out4')
    s = datetime.datetime(2022, 1, 20,0,0)
    e = datetime.datetime(2022, 2, 20,0,0)

    fake = Faker()
    timestamp = fake.date_time_between(s,e)
    time = timestamp.strftime("%m/%d/%Y %H:%M")
    path = 'decision/' + 'year=' + str(timestamp.year) + '/' + 'month=' + str(timestamp.month) + '/' + 'day=' + str(timestamp.day) + '/' + 'hour=' + str(timestamp.hour) + '/'
    if not os.path.exists(path):
        os.makedirs(path)
    with open(path+'decision_'+data['request_id']+'.json', 'w') as json_file:
        json.dump(data, json_file)

    print(os.getcwd())

#___________________________________________________________________________________________________________________________________________________________________________
#                                                                                      with our DAG
#___________________________________________________________________________________________________________________________________________________________________________

with dag:

    sensing_task = FileSensor(
        task_id = 'sensing_task',
        filepath = 'request_1411.json',
        fs_conn_id = 'my_file_system_1',
        poke_interval = 10
    )

    ReadJSON = PythonOperator(
        task_id ='ReadJSON',
        python_callable = ReadJSON,
        provide_context = True,
        retries = 10,
        retry_delay = timedelta(seconds=1) 
    )

    transform = PythonOperator(
        task_id ='transform',
        python_callable = transform,
        provide_context = True
     #   retries = 10,
     #  retry_delay = timedelta(seconds=1) 
    )
    CheckFraud = PythonOperator(
        task_id ='CheckFraud',
        python_callable = CheckFraud,
        provide_context = True
     #   retries = 10,
     #  retry_delay = timedelta(seconds=1) 
    )
    AgentCheck = PythonOperator(
        task_id ='AgentCheck',
        python_callable = AgentCheck,
        provide_context = True
     #   retries = 10,
     #  retry_delay = timedelta(seconds=1) 
    )  
    CreateDB = PythonOperator(
        task_id ='CreateDB',
        python_callable = CreateDB,
        provide_context = False
     #   retries = 10,
     #  retry_delay = timedelta(seconds=1) 
    )        
      
    FillDB = PythonOperator(
        task_id ='FillDB',
        python_callable = FillDB,
        provide_context = True
     #   retries = 10,
     #  retry_delay = timedelta(seconds=1) 
    )   

    # GetJSON = PythonOperator(
    #     task_id ='GetJSON',
    #     python_callable = GetJSON,
    #     provide_context = True
    #  #   retries = 10,
    #  #  retry_delay = timedelta(seconds=1) 
    # )  

    FinalJSON = PythonOperator(
        task_id ='FinalJSON',
        python_callable = FinalJSON,
        provide_context = True
     #   retries = 10,
     #  retry_delay = timedelta(seconds=1) 
    ) 


    sensing_task >> ReadJSON >> transform >> CheckFraud >> AgentCheck >> CreateDB >> FillDB >> FinalJSON
 
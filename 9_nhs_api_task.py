from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from myscript.carn_API_data import save_data,check_file,data_transform
from myscript.pbi_rest_api import refresh_data,realtime_data
import time




def get_data():
    save_data()

def check_file_status(ti):
    status = check_file()
    if status == "yes":
        ti.xcom_push(key='update_time',value=datetime.now())
        print("file status is ok")
    else:
        raise AirflowSkipException("Condition not met; skipping downstream task.")

def transform():
    data_transform()
    


def load_data():
    realtime_data()
    time.sleep(30)
    refresh_data()

default_parameters ={
    'owner':'amila',
    'start_date': datetime(2024,12,25),
    'schedule_interval':'0 12 * * 1-3',
    'catchup': False,
    'tags':["NHS"],
}


with DAG("nhs_api_data",default_args=default_parameters) as dag:

    task1 = PythonOperator(task_id="Extarct_data_API_to_CSV",python_callable=get_data)
    task2 = PythonOperator(task_id="File_status_chek",python_callable=check_file_status)
    task3 = PythonOperator(task_id="data_transform",python_callable=transform)
    task4 = PythonOperator(task_id="data_load",python_callable=load_data)


task1>>task2>>task3>>task4
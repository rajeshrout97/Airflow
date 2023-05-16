from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator


# There are default arguments for our Dag/Job
default_args = {
    'owner' : 'Rajesh',
    "start_date": datetime(2023, 1, 2),
    'email':'routr5953@gmail.com',
    'retries': 0,
    }


dag = DAG(
    dag_id = 'Sample-Dag',
    default_args = default_args,
    schedule_interval= None,
    catchup=False,
    )


with dag:
   
    t1 = EmptyOperator(task_id='task1')
  

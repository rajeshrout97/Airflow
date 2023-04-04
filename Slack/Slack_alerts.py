from datetime import datetime, timedelta
from airflow import DAG 
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator


### Slack Alerts #####
def slack_fail_alert(context):
    
    # we are retrieving our Slack Variable
    SLACK_CONN_ID = Variable.get('slack_conn')

    '''
    Owner - Rajesh 

    Data Scientist
    
    '''
    # we are fetching Slack connection Password
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

    # We are getting the object after making connection to Slack using Basehook
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    ti = context['ti'] # to get the Task Instance
    task_state = ti.state # To get the Task state
    

    # Here you can customize the message as per your need.
    # Task state = Success 

    if task_state == 'success':
        slack_msg = f"""
        :white_check_mark: Task Succeeded.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('execution_date')}
        """
    # Task state = Failed 

    elif task_state =='failed':
        slack_msg = f"""
        :x: Task Failed.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('execution_date')}
        <{context.get('task_instance').log_url}|*Logs*>
        """


    # its a sub task which will be executed whenever the function is called 
    
    
    slack_alert = SlackWebhookOperator(
        task_id='slack_fail',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username='airflow',
        http_conn_id=SLACK_CONN_ID
    )

    # execute method to run this task
    return slack_alert.execute(context=context)




default_args = {
    'owner':'Rajesh',
    'retries': 0,
    'start_date':datetime(2023, 3, 2),
    'on_failure_callback': slack_fail_alert,
    'on_success_callback': slack_fail_alert

   
}



dag = DAG(
    dag_id = 'Slack-Alert-DAG',
    description='Airflow DAG to send alerts to Slack channel',
    default_args = default_args,
    schedule_interval= '@daily',
    catchup=False,
    dagrun_timeout = timedelta(minutes = 10),
    tags = ['Slack Tag'],
    )


with dag:
    
    dummy_task = EmptyOperator(
        task_id = 'empty-task'
    )

    dummy_task
    
    # In case you want to check for Failure state, run the below task
    # Use this task decorator if your airflow version is 2.4.0 atleast
    '''
    @task
    def fail_task():
        raise AirflowFailException('Testing')
    
    fail_task()

    dummy_task >> fail_task()
    '''

    # else you can use Python operator for the same
    '''
    def fail_func():
        raise AirflowFailException('Testing for Failure')


    fail_task = PythonOperator(
        task_id = 'fail-task',
        python_callable = fail_func
    )

    dummy_task >> fail_task
    
    '''

  


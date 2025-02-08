from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import logging
from airflow.utils.trigger_rule import TriggerRule  # Import TriggerRule
import time
from urllib.parse import quote
import random
from dateutil import parser
from airflow.decorators import task
from pendulum import timezone as pdt
from airflow.providers.apprise.notifications.apprise import send_apprise_notification
from apprise import NotifyType
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Configuring Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
PDT_timezone = pdt("America/Los_Angeles")
start_date = PDT_timezone.datetime(year=2023, month=5, day=7)

interval = timedelta(minutes=30)
delay = 5
retry_delay = 10
max_retry_delay = 60

default_args = {
    'owner': 'Rajesh',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def delete_existing_blobs():
    container_name = Variable.get('SENDGRID_BONZO_AZURE_CONTAINER').lower()
    wasb_hook = WasbHook(wasb_conn_id='azure_conn')
    folder_name = Variable.get('SENDGRID_BONZO_AZURE_FOLDER_NAME').lower() + "/"
        
    try:
        blobs = wasb_hook.get_blobs_list(container_name=container_name)
        for blob in blobs:
            if blob.endswith('/') and blob == folder_name:
                wasb_hook.delete_file(container_name, folder_name.replace('/',''), is_prefix=True)
                logging.info(f'{folder_name} Folder Deleted from the Azure Storage Account')
    except Exception as e:
        raise AirflowException(f"Error caused while deleting the Folder. Error: {e}")
        

def convert_to_naive(dt):
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt


def fetch_data(start_time, end_time):
    api_key = Variable.get('SENDGRID_API_KEY')
    base_url = Variable.get('SENDGRID_API_BASE_URL')

    query = (f'last_event_time BETWEEN TIMESTAMP "{start_time.isoformat()}Z" '
             f'AND TIMESTAMP "{end_time.isoformat()}Z"')
    encoded_query = quote(query)
    url = f'{base_url}?limit=1000&query={encoded_query}'
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
    retry_count = 0
    max_retries = 7
    backoff_factor = 2
    logging.info(f"\n\tCode executed from {start_time} to {end_time}\n")

    while True:
        try:
            response = requests.get(url, headers=headers, verify=False)
            
            if response.status_code == 429:  # Rate limit exceeded
                retry_delay_sec = min(retry_delay * (2 ** retry_count) + random.uniform(0, 1), max_retry_delay)
                logging.warning(f'Rate limit exceeded. Waiting for {retry_delay_sec} seconds... (Retry count: {retry_count})')
                time.sleep(retry_delay_sec)
                retry_count += 1
                continue
            
            elif response.status_code == 500:  # Internal Server Error
                retry_delay_sec = min(retry_delay * (backoff_factor ** retry_count), max_retry_delay)
                logging.warning(f'Server error (500). Retrying in {retry_delay_sec} seconds... (Retry count: {retry_count})')
                time.sleep(retry_delay_sec)
                retry_count += 1
                continue
            
            else:
                response.raise_for_status()
                return response.json()
            
        except requests.exceptions.HTTPError as err:
            logging.error(f'HTTP error occurred: {err}')
            retry_count += 1
            if retry_count > max_retries:
                raise AirflowException(f'Max retries reached. Code Fails!')
            time.sleep(retry_delay * retry_count)




def fetch_and_upload_data_to_azure(**kwargs):
    api_key = Variable.get('SENDGRID_API_KEY')
    base_url = Variable.get('SENDGRID_API_BASE_URL')
    
    current_time = Variable.get('SENDGRID_API_START_DATE')
    if len(current_time) == 0:
        raise AirflowException('SENDGRID START DATE is blank. Hence, DAG failed')
    
    current_time = parser.parse(current_time)
    current_time = convert_to_naive(current_time)
    
    end_date = Variable.get('SENDGRID_API_END_DATE')
    if end_date is not None and len(end_date) > 0:
        end_date = parser.parse(end_date)
        end_date = convert_to_naive(end_date)
    else:
        end_date = datetime.now()

    end_date = convert_to_naive(end_date)

    wasb_hook = WasbHook(wasb_conn_id='azure_conn')
    container_name = 'bonzo'
    folder_name = Variable.get('SENDGRID_BONZO_AZURE_FOLDER_NAME')

    temp_interval = interval
    global_end_time = current_time + interval

    try:
        while current_time < end_date:
            next_time = current_time + temp_interval
            
            if next_time > end_date:
                next_time = end_date

            data = fetch_data(current_time, next_time)

            logging.info(f'\n\nFetched {len(data["messages"])} messages')

            if len(data["messages"]) == 1000:
                if temp_interval == timedelta(minutes=30):
                    temp_interval = timedelta(minutes=10)
                elif temp_interval == timedelta(minutes=10):
                    temp_interval = timedelta(minutes=2)
                else:
                    temp_interval = temp_interval / 2
                logging.info(f'Adjusting time interval to {temp_interval} due to response length')
                continue
            
            elif len(data["messages"]) > 0:
                blob_name = f"{folder_name}/sendgrid_data_{current_time.strftime('%Y%m%d_%H%M%S%f')}.json"
                wasb_hook.load_string(json.dumps(data, indent=4), container_name, blob_name, overwrite=True)
                logging.info(f"\n\tUploaded data from {current_time} to {next_time} to {blob_name}")

            else:
                logging.info(f"\n\tNo messages found from {current_time} to {next_time}")

            current_time = next_time

            if next_time >= global_end_time:
                temp_interval = timedelta(minutes=30)
                print('\nTime Interval reset to 30 minutes\n')
                global_end_time = global_end_time + temp_interval
            
            else:
                if temp_interval < timedelta(minutes=2):
                    temp_interval = timedelta(minutes=2)
            

            time.sleep(delay)

    except Exception as e:
        #delete_existing_blobs()
        raise AirflowException(f"Error fetching or uploading data: {e}")

    finally:
        kwargs['ti'].xcom_push(key='end_date', value=end_date)

with DAG(
    'sendgrid_data_fetch_and_upload_to_snowflake_dag',
    default_args=default_args,
    description='DAG to fetch SendGrid data and upload it to Snowflake',
    schedule_interval='0 * * * *',
    start_date=start_date,
    on_failure_callback=send_apprise_notification(
        title='🚨 **SENDGRID Master DAG Failure Alert** 🚨',
        body='''**Dag -** {{ dag.dag_id }}<br>
        **Task ID -** {{ ti.task_id }} Failed 😞<br>
        **Execution Date -**  {{ ds }}<br>
        **Log URL -** [Click here to view logs]({{ ti.log_url }}) 🏷️<br>
        ''',
        notify_type=NotifyType.FAILURE,
        apprise_conn_id='notifier_conn',
        tag=['teams_alert', 'client_teams_alert']
    ),
    tags=['SENDGRID', 'MASTER', 'BONZO', 'MESSAGES_API'],
    catchup=False,
) as dag:

    delete_blob = PythonOperator(
        task_id='delete_existing_blobs',
        python_callable=delete_existing_blobs,
        provide_context=True
    )

        
    fetch_and_upload_task = PythonOperator(
        task_id='fetch_sendgrid_api_data_and_upload_to_azure',
        python_callable=fetch_and_upload_data_to_azure,
        provide_context=True
    )


    trigger_child_dag = TriggerDagRunOperator(
        task_id='trigger_child_dag_load_data_sf',
        trigger_dag_id='azure_sendgrid_bonzo_messages_dag',
        conf={
            'expected_file_type': {'json': {'column_header_flag': 'false', 'delimeter': 'None'}}
        },
        execution_date='{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=True,
    )


    @task
    def pipeline_ends(**kwargs):
        try:
            end_date = kwargs['ti'].xcom_pull(key='end_date', task_ids="fetch_sendgrid_api_data_and_upload_to_azure")
           
            print(f'\nOld End Date :- {end_date}\n')
            end_date = end_date + timedelta(milliseconds=1)

            print(f'\nNew End Date :- {end_date}\n')
            
            Variable.set('SENDGRID_API_START_DATE', f'{end_date}')
            Variable.set('SENDGRID_API_END_DATE', '')
            
            logging.info("Dates updated successfully!")
        except Exception as e:
            
            raise AirflowException(f'Error updating dates: {e}')


    
    
    delete_blob_on_failure = PythonOperator(
        task_id='delete_blobs_on_failure',
        python_callable=delete_existing_blobs,
        trigger_rule=TriggerRule.ONE_FAILED,  # Runs only if any upstream task fails
    )
    
    def fail_dag():
        raise AirflowException("DAG failed due to a failure in an upstream task.")

    fail_dag_task = PythonOperator(
        task_id='fail_dag_task',
        python_callable=fail_dag
        
    )

    



    pipe_ends = pipeline_ends()

    delete_blob >> fetch_and_upload_task >> trigger_child_dag >> pipe_ends

   
    [fetch_and_upload_task, trigger_child_dag, pipe_ends] >> delete_blob_on_failure
    delete_blob_on_failure >> fail_dag_task

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import clickhouse_connect
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bionicpro',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def ch_create_client():
    clickhouse_host = os.getenv("CH_HOST", "clickhouse")
    clickhouse_username = os.getenv("CH_USERNAME", "admin")
    clickhouse_password = os.getenv("CH_PASSWORD", "admin")
    return clickhouse_connect.get_client(
        host=clickhouse_host,
        username=clickhouse_username,
        password=clickhouse_password
    )

def ch_execute_command(query):
    logger.info(f"ch_execute_command. query={query}")
    client = ch_create_client()
    client.command(query)
    logger.info("ch_execute_command. done")

def ch_execute_insert(table, data, column_names):
    logger.info(f"ch_execute_insert. table={table}, data={data}, column_names={column_names}")
    client = ch_create_client()
    client.insert(table, data, column_names=column_names)
    logger.info("ch_execute_insert. done")

def prepare(**context):
    query = '''
        CREATE TABLE IF NOT EXISTS reports_mart (
            user_id String,
            username String,
            email String,
            first_name String,
            last_name String,
            prosthetic_model String,
            report_date DateTime,
            thing_id String,
            thing_param_name String,
            thing_value String,
            processed_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (user_id, report_date, thing_id, thing_param_name, processed_at)
    '''
    ch_execute_command(query)

    logger.info("prepare. done")

def extract_crm_data(**context):
    crm_data = [
        {
            'user_id': 'prothetic1',
            'username': 'prothetic1',
            'email': 'prothetic1@example.com',
            'first_name': 'Prothetic',
            'last_name': 'One',
            'prosthetic_model': 'bp_v2'
        },
        {
            'user_id': 'prothetic2',
            'username': 'prothetic2',
            'email': 'prothetic2@example.com',
            'first_name': 'Prothetic',
            'last_name': 'Two',
            'prosthetic_model': 'bp_v1'
        },
        {
            'user_id': 'prothetic3',
            'username': 'prothetic3',
            'email': 'prothetic3@example.com',
            'first_name': 'Prothetic',
            'last_name': 'Three',
            'prosthetic_model': 'bp_v3'
        }
    ]

    logger.info("extract_crm_data. done")

    return crm_data

def extract_telemetry_data(**context):
    telemetry_data = {
        'prothetic1': {
            'thing_id': "1_1",
            'thing_param_name': "usage_hours",
            'thing_value': "8.1"
        },
        'prothetic2': {
            'thing_id': "2_1",
            'thing_param_name': "usage_hours",
            'thing_value': "3.1"
        },
        'prothetic3': {
            'thing_id': "3_1",
            'thing_param_name': "usage_hours",
            'thing_value': "7.1"
        }
    }

    logger.info("extract_telemetry_data. done")

    return telemetry_data

def transform_and_load(**context):
    crm_data = context['task_instance'].xcom_pull(task_ids='extract_crm_data')
    telemetry_data = context['task_instance'].xcom_pull(task_ids='extract_telemetry_data')

    report_date = datetime.now()

    for crm_i in crm_data:
        user_id = crm_i['user_id']
        telemetry_i = telemetry_data.get(user_id, {})
        row = [crm_i['user_id'], crm_i['username'], crm_i['email'], crm_i['first_name'], crm_i['last_name'], crm_i['prosthetic_model'], report_date, telemetry_i.get('thing_id', ""), telemetry_i.get('thing_param_name', ""), telemetry_i.get('thing_value', "")]
        data = [row]
        column_names = ["user_id", "username", "email", "first_name", "last_name", "prosthetic_model", "report_date", "thing_id", "thing_param_name", "thing_value"]
        ch_execute_insert("reports_mart", data, column_names)

    logger.info("transform_and_load. done")

def validate_data(**context):
    logger.info("validate_data. done")

with DAG(
        dag_id='bp_etl_reports',
        default_args=default_args,
        description='BP ETL reports process',
        schedule=timedelta(minutes=2),
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
) as dag:

    prepare_task = PythonOperator(
        task_id='prepare',
        python_callable=prepare,
    )

    extract_crm_data_task = PythonOperator(
        task_id='extract_crm_data',
        python_callable=extract_crm_data,
    )

    extract_extract_telemetry_data_task = PythonOperator(
        task_id='extract_telemetry_data',
        python_callable=extract_telemetry_data,
    )

    transform_load_task = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load,
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    prepare_task >> [extract_crm_data_task, extract_extract_telemetry_data_task]
    [extract_crm_data_task, extract_extract_telemetry_data_task] >> transform_load_task >> validate_task

import os
from datetime import datetime
import json
from pathlib import Path

import snowflake.connector
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sensors.base import PokeReturnValue

NAME = 'carl'  # TODO 0: input your name to differentiate your table from others'
TABLE_NAME = f'airflow_lab_{NAME}'


def get_snowflake_connection():
    # TODO 1: Specify Airflow variables. Import them here.
    return snowflake.connector.connect(
        user= Variable.get('SNOWFLAKE_USER'),
        password=Variable.get('SNOWFLAKE_PASSWORD'),
        account=Variable.get('SNOWFLAKE_ACCOUNT'),
        database='DB_LAB_M1W4_CARL',
        schema='SC_LAB_M1W4_DEMO_CARL',
    )


def get_snowflake_hook():
    # TODO 2: specify Snowflake connection
    return SnowflakeHook(snowflake_conn_id='snowflake_academy')

@task()
def create_table():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute(f"CREATE OR REPLACE TABLE {TABLE_NAME} (id STRING, data INT)")
    conn.commit()
    cur.close()
    conn.close()
    
@task.sensor(poke_interval=15, timeout=120)
def check_for_json_data() -> list[str]:
    # Search for JSON files matching the pattern 'data*.json'
    json_files = [str(file) for file in Path(".").rglob("data*.json")]
    
    # If no files found, raise an exception to retry
    if not json_files:
        raise ValueError("No JSON files found. Retrying...")
    
    # Return the list of file paths if files are found
    return json_files


def parse_json(path: str) -> tuple:
    row_data = json.loads(Path(path).read_text())
    return (row_data['id'], row_data['data'])

@task
def parse_json_data(files: list[str]) -> list[tuple]:
    if not files:  # Add a safeguard check
        raise ValueError("No files received for parsing.")
    
    parsed_rows = [parse_json(file) for file in files]
    return parsed_rows



@task
def insert_data(row: tuple):
    '''
    DO NOT modify this task
    '''
    id, data = row
    hook = get_snowflake_hook()
    hook.run(f"INSERT INTO {TABLE_NAME} values ('{id}', {data})")

'''
TODO 4: Notification
Complete the folllowing 2 functions.

The first one is a task, which is to be placed at the end of dag to signal successful run
The second one is a callback function, which will be placed in DAG config

hint: the bash command to send notification to discord server is
curl -H 'Content-Type: application/json' -X POST -d '{"content": "This string will be sent to discord channel"} $discord_webwooh

Also since there are multiple students using the same Discord webhook, it's a good idea to 
include your name in the message to distinguise from others.
'''


@task
def dag_success_notification():
    os.system(
        "curl -H 'Content-Type: application/json' -X POST -d "
        "'{\"content\": \"DAG successfully completed by Carl! 🎉\"}' "
        "$discord_webhook"
    )


def discord_on_dag_failure_callback(context):
    os.system(
        "curl -H 'Content-Type: application/json' -X POST -d "
        "'{\"content\": \"DAG failed, Carl! 😢 Check the logs for details.\"}' "
        "$discord_webhook"
    )


@dag(
    dag_id="airflow_lab",
    schedule_interval=None,
    start_date=datetime(2025, 3, 1),
    catchup=False,
    on_failure_callback=discord_on_dag_failure_callback,
)
def airflow_lab():
    '''
    TODO 5: Connect all the tasks
    
    Think of the most appropriate task orders to build your dag and declare the dependencies here
    Some of the tasks are dynamics
    
    Remember to place dag_success_notification() at the end of the dag, and use the failure_callback() function for dag.
    '''
    # Task Definitions
    create_table_task = create_table()
    json_files = check_for_json_data()
    parsed_data = parse_json_data(json_files)
    


airflow_lab()
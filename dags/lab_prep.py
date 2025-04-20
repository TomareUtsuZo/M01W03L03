from datetime import datetime
from random import randint
from uuid import uuid4
import json
from pathlib import Path

from airflow.decorators import task, dag

@task
def generate_random_data() -> dict:
    return {
        'id': str(uuid4()),
        'data': randint(0, 999),
    }

@task
def write_json_data(data):
    Path(f'data_{randint(1, 1000)}.json').write_text(json.dumps(data))

@dag(
    dag_id="airflow_prep",
    schedule_interval=None,
    start_date=datetime(2025, 3, 1),
    catchup=False,
)
def airflow_prep():
    write_json_data(generate_random_data())
    
airflow_prep()
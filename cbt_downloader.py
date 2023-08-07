# -*- coding: utf-8 -*-
import os
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

DAG_NAME = str(os.path.basename(__file__).split('.')[0])
OWNER = 'airflow'
DEPENDS_ON_PAST = True
POOL = 'downloads_pool'
RETRIES = int(Variable.get('gv_dag_retries'))

start_dt = pendulum.datetime(2023, 7, 6).astimezone()
# setting default arguments of dag
default_args = {
    'owner': OWNER,
    'depends_on_past': DEPENDS_ON_PAST,
    'start_date': start_dt,
    'retries': RETRIES,
    'pool': POOL
}

# Creating DAG with parameters
dag = DAG(DAG_NAME, default_args=default_args, schedule_interval="0 4 * * *", max_active_runs=1)
dag.doc_md = __doc__

cbt_start = DummyOperator(
    task_id='cbt_start',
    dag=dag
)

cbt_end = DummyOperator(
    task_id='cbt_end',
    dag=dag
)

script_full_path = os.path.join(os.environ['AIRFLOW_HOME'], 'scripts', 'downloads', 'aero_test_connector.py')

cbt_cmd = """
python3 {{ params.path }}
"""

cbt_downloader = BashOperator(
    task_id='cbt_downloader',
    bash_command=cbt_cmd,
    params={
        'path': script_full_path
    },
    wait_for_downstream=True,
    dag=dag
)

cbt_start.set_downstream(cbt_downloader)
cbt_downloader.set_downstream(cbt_end)

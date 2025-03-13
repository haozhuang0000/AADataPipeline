from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os 
import sys
path = os.environ['AIRFLOW_HOME']

default_args = {
                'owner': 'KomalChandiramani',
                'depends_on_past': False,
                'email': ['e1503332@u.nus.edu.sg'],
                'email_on_failure': True,
                'email_on_retry': True,
                'retries': 0,
                'retry_delay': timedelta(minutes=1)
                }


# Define the DAG, its ID and when should it run.
dag = DAG(
            dag_id='secfiling_dag',
            start_date=datetime(year=2025, month=1, day=10, hour=16, minute=30),
            schedule_interval="0 0 L * *", # run on the last day of the month
            default_args=default_args,
            catchup=False
            )


run_rss_feed = BashOperator(
    task_id='get_data',
    bash_command=(
        f'python {path}/dags/rss_feed.py --type 10-K --date "{{{{ macros.ds_format(ds, \'%Y-%m-%d\', \'%Y-%m-01\') }}}}"'
    ),
    dag=dag,
)

run_extract_items = BashOperator(
    task_id='extract_items',
    bash_command= f'python {path}/dags/extract_items.py',
    dag=dag,
)



run_rss_feed >> run_extract_items
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime

# path = 'D:/MyGithub/Airflow/docker/airflow/dags/src'
path = os.environ['AIRFLOW_HOME']

#dag.py
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
            dag_id='10K_Production',
            start_date=datetime(year=2025, month=1, day=27, hour=15, minute=30),
            schedule_interval='0 */3 * * *',
            default_args=default_args,
            catchup=False
            )


# Define the task 1 (collect the data) id. Run the bash command because the task is in a .py file.
task1 = BashOperator(
                        task_id='get_data',
                        bash_command=f'python {path}/dags/src/SEC_10K_Scraper.py --date "1950-01-01" --type 10-Q --scrape_method selenium',
                        dag=dag
                    )


# Define the task 2 extract items from data. Run the bash command because the task is in a .py file.
task2 = BashOperator(
                        task_id='extract_items',
                        bash_command=f'python {path}/dags/src/Extract_Items.py',
                        dag=dag
                    )

task1 >> task2


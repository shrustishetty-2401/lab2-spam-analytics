from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

DBT_BIN = '/usr/local/bin/dbt'
DBT_PROJECT_DIR = '/opt/airflow/dags/spam_detection_dbt'
DBT_PROFILES_DIR = '/opt/airflow/dags/dbt_profiles'

with DAG(
    dag_id='dbt_spam_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Run dbt models for spam email analytics',
    tags=['dbt', 'spam', 'snowflake'],
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'{DBT_BIN} run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'{DBT_BIN} test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}',
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'{DBT_BIN} snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}',
    )

    dbt_run >> dbt_test >> dbt_snapshot

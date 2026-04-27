from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='hello_goodbye_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    hello_task = DockerOperator(
        task_id='hello_task',
        image='hello-img',
        api_version='auto',
        auto_remove=True,
        command="python print_hello.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        do_xcom_push=True
    )

    goodbye_task = DockerOperator(
        task_id='goodbye_task',
        image='goodbye-img',
        api_version='auto',
        auto_remove=True,
        command="python print_goodbye.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        do_xcom_push=True
    )

    hello_task >> goodbye_task
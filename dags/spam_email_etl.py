from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import csv

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def load_csv_to_snowflake():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    cursor = conn.cursor()

    csv_path = '/opt/airflow/spam mail.csv'

    try:
        cursor.execute("USE ROLE TRAINING_ROLE")
        cursor.execute("USE WAREHOUSE RABBIT_QUERY_WH")
        cursor.execute("USE DATABASE USER_DB_RABBIT")
        cursor.execute("USE SCHEMA PUBLIC")
        cursor.execute("BEGIN")
        cursor.execute("TRUNCATE TABLE RAW_SPAM_EMAILS")

        with open(csv_path, 'r', encoding='latin-1') as f:
            reader = csv.DictReader(f)
            batch = []
            for row in reader:
                label = row.get('Category', '').strip()
                message = row.get('Masseges', '').strip()
                batch.append((label, message))

                if len(batch) >= 500:
                    cursor.executemany(
                        "INSERT INTO RAW_SPAM_EMAILS (LABEL, MESSAGE) VALUES (%s, %s)",
                        batch
                    )
                    batch = []

            if batch:
                cursor.executemany(
                    "INSERT INTO RAW_SPAM_EMAILS (LABEL, MESSAGE) VALUES (%s, %s)",
                    batch
                )

        cursor.execute("COMMIT")
        print("Successfully loaded spam emails to Snowflake")

    except Exception as e:
        cursor.execute("ROLLBACK")
        raise Exception(f"Failed to load data: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def verify_load():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM USER_DB_RABBIT.PUBLIC.RAW_SPAM_EMAILS")
        count = cursor.fetchone()[0]
        print(f"Total rows loaded: {count}")
        cursor.execute("""
            SELECT LABEL, COUNT(*) as COUNT
            FROM USER_DB_RABBIT.PUBLIC.RAW_SPAM_EMAILS
            GROUP BY LABEL
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} emails")
    finally:
        cursor.close()
        conn.close()

with DAG(
    dag_id='spam_email_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='ETL pipeline to load spam email data into Snowflake',
    tags=['spam', 'etl', 'snowflake'],
) as dag:

    load_task = PythonOperator(
        task_id='load_csv_to_snowflake',
        python_callable=load_csv_to_snowflake,
    )

    verify_task = PythonOperator(
        task_id='verify_load',
        python_callable=verify_load,
    )

    load_task >> verify_task

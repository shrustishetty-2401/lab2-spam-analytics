from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import requests
import json


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}


with DAG(
    dag_id="country_capital_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:


    @task
    def extract():

        lat = Variable.get("latitude")
        lon = Variable.get("longitude")

        print("Latitude:", lat)
        print("Longitude:", lon)

        url = Variable.get("country_capital_url")

        response = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30
        )

        print("STATUS:", response.status_code)
        print("RAW RESPONSE SAMPLE:", response.text[:200])

        if response.status_code != 200:
            raise Exception("API request failed")

        data = json.loads(response.text)

        records = []

        for c in data:
            country = c.get("name", {}).get("common", "")
            capital_list = c.get("capital", [])
            capital = capital_list[0] if capital_list else ""

            records.append((country, capital))

        return records


    @task
    def transform(records):

        clean = []

        for r in records:
            country = r[0]
            capital = r[1]

            if country and capital:
                clean.append((country, capital))

        return clean


    @task
    def load(records):

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

        conn = hook.get_conn()
        cursor = conn.cursor()

        # start transaction
        cursor.execute("BEGIN")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS COUNTRY_CAPITALS (
                COUNTRY STRING,
                CAPITAL STRING
            )
        """)

        # full refresh
        cursor.execute("TRUNCATE TABLE COUNTRY_CAPITALS")

        for country, capital in records:
            cursor.execute(
                "INSERT INTO COUNTRY_CAPITALS (COUNTRY, CAPITAL) VALUES (%s, %s)",
                (country, capital)
            )

        # commit transaction
        cursor.execute("COMMIT")

        print("Full refresh completed")

        return "DONE"


    data = extract()
    cleaned = transform(data)
    load(cleaned)
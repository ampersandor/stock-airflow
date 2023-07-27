import requests
import mysql.connector
from datetime import datetime
from datetime import timedelta
import requests
import psycopg2
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook

def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
                upload_date DATE DEFAULT CURRENT_DATE,
                upload_time TIME DEFAULT CURRENT_TIME,
                date DATE,
                time TIME,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume INT
        );
    """
    logging.info(query)
    cur.execute(query)

def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id = 'postgres_docker')
    return hook.get_conn().cursor()

@task
def extract(url, symbol, apikey, interval):
    api = f"{url}&symbol={symbol}&interval={interval}&apikey={apikey}"
    r = requests.get(api)
    data = r.json()
    return data[f"Time Series ({interval})"]

@task
def transform(data):
    transformed_data = list()
    for key, val in data.items():
        date, time = key.split()
        op, hi, lo, cl, vo = val["1. open"], val["2. high"], val["3. low"], val["4. close"], val["5. volume"]
        transformed_data.append((date, time, op, hi, lo, cl, vo))
    return transformed_data

@task
def load(schema, table, data, drop_first=True):
    cur = get_postgres_connection()
    try:
        cur.execute("BEGIN;")
        _create_table(cur, schema, table, drop_first)

        for row in data:
            query = f"""
                INSERT INTO stock.apple (date, time, open, high, low, close, volume)
                VALUES ('{row[0]}', '{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]})
            """
            print(query)
            cur.execute(query)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise 
    logging.info("load done")


with DAG(
    dag_id = "apple stock",
    start_date=datetime(2021, 9, 13),
    schedule="0 0 1 * *", # every month (day 1, 00:00)
    max_active_runs=1,
    tags=['ODIGODI', 'officetel', "ETL"],
    catchup=True,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
    }
) as dag:
    schema = "dev"
    table = "stock"
    symbol = "AAPL"
    interval = "5min"
    url = Variable.get("stock_url")
    apikey = Variable.get("stock_api_key")

    data = extract(url, symbol, apikey, True)
    data = transform(data)
    load(schema, table, data)
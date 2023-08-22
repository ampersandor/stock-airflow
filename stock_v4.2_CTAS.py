import requests
import mysql.connector
from datetime import datetime
from datetime import timedelta
import requests
import psycopg2
import logging
from time import time

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
                volume INT,
                CONSTRAINT pk_columns PRIMARY KEY (date, time)
        );
    """

    logging.info(query)

    cur.execute(query)


def _create_temp_table(cur, schema, table):
    ctas_query = f"""
    CREATE TEMPORARY TABLE temp_table AS
        SELECT *
        FROM {schema}.{table} WHERE 1 = 0;
    """
    logging.info(ctas_query)

    cur.execute(ctas_query)


def get_postgres_connection():
    hook = PostgresHook(postgres_conn_id="postgres_docker")
    return hook.get_conn().cursor()


@task
def extract(url, symbol, interval, apikey, **kwargs):
    execution_date = kwargs["logical_date"]
    year_month = execution_date.strftime("%Y-%m")
    api = f"{url}&symbol={symbol}&interval={interval}&month={year_month}&apikey={apikey}"
    print(api)
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
def load(schema, table, data, drop_first=False):
    s_time = time()
    cur = get_postgres_connection()
    try:
        cur.execute("BEGIN;")
        _create_table(cur, schema, table, drop_first)
        for row in data:
            query = f"""
                INSERT INTO {schema}.{table} (date, time, open, high, low, close, volume)
                VALUES ('{row[0]}', '{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]})
                ON CONFLICT (date, time) DO NOTHING;
            """
            print(query)
            cur.execute(query)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")
    print(time() - s_time)  # 0.4456489086151123 | 0.5138828754425049 | 0.4215128421783447


@task
def load_CTAS(schema, table, data, drop_first=False):
    s_time = time()
    cur = get_postgres_connection()
    try:
        cur.execute("BEGIN;")
        _create_table(cur, schema, table, drop_first)
        _create_temp_table(cur, schema, table)
        for row in data:
            query = f"""
                INSERT INTO temp_table (date, time, open, high, low, close, volume)
                VALUES ('{row[0]}', '{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]})
            """
            print(query)
            cur.execute(query)

        insert_to_original = f"""
            INSERT INTO {schema}.{table} (date, time, open, high, low, close, volume)
            SELECT t.date, t.time, t.open, t.high, t.low, t.close, t.volume
            FROM temp_table t
            LEFT JOIN {schema}.{table} o ON t.date = o.date AND t.time = o.time
            WHERE o.time IS NULL;
        """
        print(insert_to_original)
        cur.execute(insert_to_original)
        drop_temp_table = "DROP TABLE temp_table;"
        cur.execute(drop_temp_table)
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise
    logging.info("load done")
    print(time() - s_time)  # 0.4874730110168457 | 0.3664379119873047 | 0.45998477935791016


with DAG(
    dag_id="apple-stock-CTAS",
    start_date=datetime(2023, 1, 1),
    schedule="0 0 1 * *",  # every month (day 1, 00:00)
    max_active_runs=1,
    tags=["stock", "apple", "ETL"],
    catchup=True,
    default_args={
        "retries": 12,
        "retry_delay": timedelta(seconds=30),
    },
) as dag:
    schema = "stock"
    table = "apple"
    symbol = "AAPL"
    interval = "5min"
    url = Variable.get("stock_url")
    apikey = Variable.get("stock_api_key")

    data = extract(url, symbol, interval, apikey)
    data = transform(data)
    load_CTAS(schema, table, data)

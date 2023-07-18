import requests
import mysql.connector
from mysql.connector import errorcode
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def extract(**context):
    symbol = context["params"]["symbol"]
    apikey = context["params"]["apikey"]
    interval = context["params"]["interval"]
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={apikey}'
    r = requests.get(url)
    data = r.json()
    return data[f"Time Series ({interval})"]

def transform(**context):
    data = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    transformed_data = list()
    for key, val in data.items():
        date, time = key.split()
        op, hi, lo, cl, vo = val["1. open"], val["2. high"], val["3. low"], val["4. close"], val["5. volume"]
        transformed_data.append((date, time, op, hi, lo, cl, vo))
    return transformed_data

def load(**context):
    data = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    user, password, host, port = context["params"]["user"], context["params"]["password"], \
                                context["params"]["host"], context["params"]["port"]
    try:
        connection = mysql.connector.connect(
            user=user,
            password=password,
            host=host,
            port=port,
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
        raise
    else:
        cur = connection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS stock.apple;")
        query = f"""
            CREATE TABLE IF NOT EXISTS stock.apple (
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
        cur.execute(query)
        for row in data:
            query = f"""
                INSERT INTO stock.apple (date, time, open, high, low, close, volume)
                VALUES ('{row[0]}', '{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]})
            """
            print(query)
            cur.execute(query)
        connection.commit()
        connection.close()

dag = DAG(
    dag_id = 'Apple_Stock',
    start_date = datetime(2023,7,18),
    catchup=False,
    tags=['example'],
    schedule = '0 2 * * *'
    )

extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {"symbol": "AAPL", "apikey": "HZUQLDZQ3ZW5NTRB", "interval": "5min"},
    dag = dag)

transform = PythonOperator(
    task_id = 'transform',
    python_callable = transform,
    dag = dag)

load = PythonOperator(
    task_id = 'load',
    python_callable = load,
    params = {
        'user': 'airflow',
        'password': 'airflow',
        'host': '10.11.184.183',
        'port': '3306'
    },
    dag = dag)

extract >> transform >> load
import requests
import mysql.connector
from mysql.connector import errorcode

apikey = "HZUQLDZQ3ZW5NTRB"
url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=AAPL&interval=5min&apikey={}'

r = requests.get(url.format(apikey))

raw_data = r.json()


time_data = raw_data["Time Series (5min)"]

print(time_data)

try:
    connection = mysql.connector.connect(
        user='airflow',
        password='airflow',
        host='localhost',
        port='3306',
    )
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
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
    print("load!")
    for key, val in time_data.items():
        date, time = key.split()
        op, hi, lo, cl, vo = val["1. open"], val["2. high"], val["3. low"], val["4. close"], val["5. volume"]
        query = f"""
            INSERT INTO stock.apple (date, time, open, high, low, close, volume)
            VALUES ('{date}', '{time}', {op}, {hi}, {lo}, {cl}, {vo})
        """
        print(query)
        cur.execute(query)
    connection.commit()
    connection.close()
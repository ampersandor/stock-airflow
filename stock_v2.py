import requests
import mysql.connector
from mysql.connector import errorcode

apikey = "?"
def extract(symbol, apikey, interval):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={apikey}'
    r = requests.get(url)
    data = r.json()
    return data[f"Time Series ({interval})"]

def transform(data):
    transformed_data = list()
    for key, val in data.items():
        date, time = key.split()
        op, hi, lo, cl, vo = val["1. open"], val["2. high"], val["3. low"], val["4. close"], val["5. volume"]
        transformed_data.append((date, time, op, hi, lo, cl, vo))
    return transformed_data

def load(data, user="airflow", password="airflow", host="localhost", port="3306"):
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


if __name__ == "__main__":
    data = extract("AAPL", apikey, "5min")
    data = transform(data)
    load(data)

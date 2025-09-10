import azure.functions as func
import datetime
import json
import logging
import requests
import pyodbc
import os

app = func.FunctionApp()

# API strings for fetching
API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]
BASE_URL = "https://www.alphavantage.co/query"

server = os.environ["SQL_SERVER"]
database = os.environ["SQL_DB"]
username = os.environ["SQL_USER"]
password = os.environ["SQL_PASSWORD"]

@app.timer_trigger(schedule="0 0 23 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def DailyStockFetcher(myTimer: func.TimerRequest) -> None:
    
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')


    try:
        # 1. Fetch stock data from Alpha Vantage
        ticker = "MSFT"
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": ticker,
            "apikey": API_KEY,
            "outputsize": "compact" # "compact" = last 100 data points; full = full history
        }

        response = requests.get(BASE_URL, params=params)
        data = response.json()

        if "Time Series (Daily)" not in data:
            logging.error("Error fetching data: %s", data)
            return

        # Get the latest available date
        time_series = data["Time Series (Daily)"]
        latest_date = sorted(time_series.keys())[-1]
        latest_data = time_series[latest_date]

        stock_data = {
            "ticker": ticker,
            "trade_date": latest_date,
            "open": float(latest_data["1. open"]),
            "high": float(latest_data["2. high"]),
            "low": float(latest_data["3. low"]),
            "close": float(latest_data["4. close"]),
            "volume": int(latest_data["5. volume"])
        }

        # Build connection string
        conn_str = (
            f"Driver={{ODBC Driver 18 for SQL Server}};"
            f"Server={server};"
            f"Database={data};"
            f"Uid={username};"
            f"Pwd={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Check table structure first
        cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'stock_prices'")
        columns = [row[0] for row in cursor.fetchall()]
        logging.info(f"Available columns: {columns}")

        # Insert a row only if it doesn’t exist already
        cursor.execute("""
            SELECT 1 FROM stock_prices WHERE ticker=? AND trade_date=?
        """, (stock_data["ticker"], stock_data["trade_date"]))

        if not cursor.fetchone():
            # 2. Insert row
            cursor.execute("""
                INSERT INTO stock_prices (ticker, trade_date, [open_price], [high_price], [low_price], [close_price], [volume])
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (stock_data["ticker"], stock_data["trade_date"],
                  stock_data["open"], stock_data["high"], stock_data["low"],
                  stock_data["close"], stock_data["volume"]))
            logging.info(f"Inserted stock data for {ticker} on {latest_date}")
        conn.commit()
        cursor.close()
        conn.close()

        logging.info(f"Entry already exists for: {ticker} on {latest_date}")

    except Exception as e:
        logging.error(f"Error in DailyStockFetcher: {e}")
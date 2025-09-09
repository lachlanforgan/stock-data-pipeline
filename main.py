# PROJECT: DATA PIPELINE PROJECT
#
# DESCRIPTION: Data pipeline for visualizing daily updates
#              in stock market. Accesses Alpha Vantage API
#
# Author: Lachlan Forgan

import requests
import pandas as pd
import pyodbc

from datetime import datetime

# Azure SQL connection details:
server = "server9697.database.windows.net"
database = "StocksVisualizerDB"
username = "lachlanf"
password = "Chelmsford2323!!"
driver = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:server9697.database.windows.net,1433;Database=StocksVisualizerDB;Uid=lachlanf;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

# Alpha Vantage Stocks API details
API_KEY = "fMAA2h8YCHW3s6SbjKGC1TJzpMuNyEm_"
BASE_URL = "https://www.alphavantage.co/query"

def fetch_daily_stock_price(ticker: str):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": ticker,
        "apikey": API_KEY,
        "outputsize": "compact" # "compact" = last 100 data points; full = full history
    }

    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if "Time Series (Daily)" not in data:
        print("Error fetching data:", data)
        return None

    # Get the latest available date
    time_series = data["Time Series (Daily)"]
    latest_date = sorted(time_series.keys())[-1]
    latest_data = time_series[latest_date]

    # Convert to cleaner format
    result = {
        "ticker": ticker,
        "trade_date": latest_date,
        "open": float(latest_data["1. open"]),
        "high": float(latest_data["2. high"]),
        "low": float(latest_data["3. low"]),
        "close": float(latest_data["4. close"]),
        "volume": int(latest_data["5. volume"])
    }

    return result

def insert_into_sql(data):
    #conn = pyodbc.connect(
     #   f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    #)
    conn = pyodbc.connect(
        "DRIVER=ODBC Driver 18 for SQL Server;"
        "SERVER=tcp:server9697.database.windows.net,1433;"
        "DATABASE=StocksVisualizerDB;"
        "UID=lachlanf;"
        "PWD=Chelmsford2323!!;"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    cursor = conn.cursor()

    # Check if record already exists
    cursor.execute("""
            SELECT COUNT(*) FROM stock_prices
            WHERE ticker = ? AND trade_date = ?
            """, (data["ticker"], data["trade_date"]))

    exists = cursor.fetchone()[0] > 0

    # Insert data into table

    if exists == 0:
        cursor.execute("""
                  INSERT INTO stock_prices (ticker, trade_date, open_price, high_price, low_price, close_price, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (data["ticker"], data["trade_date"], data["open"], data["high"], data["low"], data["close"], data["volume"]))
        print("fInserted {data['ticker']} {data['trade_date']}")
    else:
        print("Skipped {data['ticker']} {data['trade_date']} (already exists)")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    stock_data = fetch_daily_stock_price("MSFT")
    print(stock_data)

    if stock_data:
        insert_into_sql(stock_data)
        print("Inserted into SQL Database")

    # Save to CSV as well
    df = pd.DataFrame([stock_data])
    df.to_csv("stock_data.csv", index=False)
    print("Saved to stock_data.csv")
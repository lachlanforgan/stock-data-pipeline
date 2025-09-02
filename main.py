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
server = "server10050.database.windows.net"
database = "StocksPipelineDB"
username = "lachlanf"
password = "Chelmsford2323!!"
driver = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:server10050.database.windows.net,1433;Database=StocksPipelineDB;Uid=lachlanf;Pwd={your_password_here};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

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
        "date": latest_date,
        "open": float(latest_data["1. open"]),
        "high": float(latest_data["2. high"]),
        "low": float(latest_data["3. low"]),
        "close": float(latest_data["4. close"]),
        "volume": int(latest_data["5. volume"])
    }

    return result

def insert_into_sql(data):
    conn = pyodbc.connect(
        f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    cursor = conn.cursor()

    cursor.execut("""
                  INSERT INTO stock_prices (ticker, trade_date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (data["ticker"], data["trade_date"], data["open"], data["high"], data["low"], data["close"], data["volume"]))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    stock_data = fetch_daily_stock_price("MSFT")
    print(stock_data)
    df = pd.DataFrame([stock_data])
    df.to_csv("stock_data.csv", index=False)
    print("Saved to stock_data.csv")
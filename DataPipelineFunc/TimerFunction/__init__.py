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
SELECTION_MODE = os.environ["SELECTION_MODE"]
WATCHLIST_STOCKS = "MSFT,AAPL,GOOGL, NVDA"  # Comma-separated list for static mode
MAX_DAILY_STOCKS = 10

def main(myTimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    required_env_vars = ["ALPHAVANTAGE_API_KEY", "SQL_SERVER", "SQL_DB", "SQL_USER", "SQL_PASSWORD"]
    missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
    if missing_vars:
        logging.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return

    try:
        selected_stocks = select_stocks_for_processing()
        for ticker in selected_stocks:
            logging.info(f"Processing stock: {ticker}")
            process_stock_data(ticker)
    except Exception as e:
        logging.error(f"Unhandled error in main function: {e}")

# optons: "top_gainers", "top_losers", "most_actively_traded"
def get_market_movers(category="top_gainers", count=5):
    """ Fetch market movers from Alpha Vantage API"""
    params = {
        "function": "TOP_GAINERS_LOSERS",
        "apikey": API_KEY,
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()
    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching data for {category}")
        return
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP error fetching {category}: {e}")
        return
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {category}: {e}")
        return

    # Error messages for: error, note (limit hit), missing main data
    if "Error Message" in data:
        logging.error(f"API error for {category}: {data['Error Message']}")
        return
    elif "Note" in data:
        logging.error(f"API rate limit hit for {category}: {data['Note']}")
        return

    # put data into dict
    movers = data[category]

    if category in ("top_gainers", "top_losers"):
        # sort by absolute percentage change
        sorted_movers = sorted(movers,    # could be "top_gainers" or "top_losers"
                               key=lambda x: abs(float(x["change_percentage"].strip('%'))),
                               reverse=True     # largest moves first
                               )
    elif category == "most_actively_traded":
        # sort by volume
        sorted_movers = sorted(
            movers,
            key = lambda x: int(x["volume"]),   # string to int
            reverse=True
        )
    else:
        logging.error(f"Invalid category: {category}")
        return

    # return ticker list
    return [entry["ticker"] for entry in movers[:count]]

def select_stocks_for_processing():
    """Determine which stocks to process today"""
    mode = os.environ.get("SELECTION_MODE", "hybride") # default to "hybride" if not set

    if mode == "static":
        return ["MSFT", "AAPL", "GOOGL"]
    elif mode == "dynamic":
        return get_market_movers("top_gainers", 10)     # for now, get 10 of top gainers if dynamic
    else:   # hybrid
        static_stocks = ["MSFT", "AAPL", "GOOGL"]
        dynamic_stocks = get_market_movers("top_gainers", 7)  # get 7 of top gainers if hybrid
        return static_stocks + dynamic_stocks

def validate_stock_data(data_dict):
    required_fields = ["open", "high", "low", "close", "volume"]
    for field in required_fields:
        if field not in data_dict or data_dict[field] is None:
            raise ValueError(f"Missing required field: {field}")

        # Validate numeric values
        if field != "volume" and (data_dict[field] <= 0):
            raise ValueError(f"Invalid value for {field}: {data_dict[field]}")

def process_stock_data(ticker):
    """Separate function for processing individual stocks"""
    try:
        # 1. Fetch stock data from Alpha Vantage API
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": ticker,
            "apikey": API_KEY,
            "outputsize": "compact"  # "compact" = last 100 data points; full = full history
        }

        # try to fetch data with error handling
        # Handles: connection timeout, http error, and other
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()  # Raise an error for bad status codes
            data = response.json()
        except requests.exceptions.Timeout:
            logging.error(f"Timeout fetching data for {ticker}")
            return
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error fetching {ticker}: {e}")
            return
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {ticker}: {e}")
            return

        # Error messages for: error, note (limit hit), missing main data
        if "Error Message" in data:
            logging.error(f"API error for {ticker}: {data['Error Message']}")
            return
        elif "Note" in data:
            logging.error(f"API rate limit hit for {ticker}: {data['Note']}")
            return
        elif "Time Series (Daily)" not in data:
            logging.error(f"Unexpected API response structure for {ticker}: {list(data.keys())}")
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
            f"Database={database};"
            f"Uid={username};"
            f"Pwd={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

        conn = None
        cursor = None
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            # Check table structure first
            cursor.execute(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'stock_prices'")
            columns = [row[0] for row in cursor.fetchall()]
            logging.info(f"Available columns: {columns}")

            # Insert a row only if it doesn’t exist already
            cursor.execute("""
                        SELECT 1 FROM stock_prices WHERE ticker=? AND trade_date=?
                    """, (stock_data["ticker"], stock_data["trade_date"]))

            try:
                validate_stock_data(stock_data)
            except ValueError as e:
                logging.error(f"Data validation failed for {ticker} on {latest_date}: {e}")
                return

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
            else:
                logging.info(f"Stock data for {ticker} on {latest_date} already exists. Skipping insert.")

            logging.info(f"Entry already exists for: {ticker} on {latest_date}")
        except pyodbc.Error as e:
            logging.error(f"Database error: {e}")
            if conn:
                conn.rollback()
        except Exception as e:
            logging.error(f"Unexpected database error: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        logging.info(f"Successfully processed {ticker}")
    except Exception as e:
        logging.error(f"Failed to process {ticker}: {e}")
        raise  # Re-raise to be caught by main


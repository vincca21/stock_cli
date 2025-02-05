# scrape_work.py


# Part 1: Fetching Stock Data -> separate into sections based on how often the data is/would be updated -> save to json file

"""
Description:
- Use yahooquery to fetch stock price and key data from Yahoo Finance API.
- Store and organize the data points with timestamps, keeping the most recent at the top.


Review:
- The script fetches stock data using the Yahoo Finance API through the yahooquery library.
- The script fetches different types of data for each stock ticker.
- The fetched data is saved to a JSON file for each stock ticker.
- The script logs the data fetching process and any errors that occur.

Output Data:
- The script saves the fetched data to a JSON file in the data directory.
- The JSON file contains the fetched data for each stock ticker.
- The JSON file is named stock_data.json.

Improvements:
- Add more error handling to the script.
- Add more stock tickers to the ticker list for testing.
- Improve the efficiency of the data fetching process.
"""

import os
import datetime
import json
from yahooquery import Ticker  # More reliable than scraping HTML
from logs.logging import get_logger

# Set up logger
logger = get_logger()

# Ticker list for testing
ticker_list = ['AAPL', 'GOOGL', 'AMZN'] #, 'MSFT', 'TSLA', 'BRK-A', 'NVDA', 'JPM', 'V', 'UNH']

# Directory for storing data
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
DATA_FILE = os.path.join(DATA_DIR, "stock_data.json")

def fetch_live_data(ticker):
    """
    Fetch live stock data for a given ticker using yahooquery.
    
    Args:
    - ticker (str): Stock ticker symbol.
    
    Returns:
    - dict: Live stock data with price, change, percentage change, and timestamp.
    """
    try:
        stock = Ticker(ticker)
        market_data = stock.price.get(ticker, {})
        live_data = {
            'price': market_data.get('regularMarketPrice'),
            'change': market_data.get('regularMarketChange'),
            'percent_change': market_data.get('regularMarketChangePercent'),
            'timestamp': datetime.datetime.now().isoformat()
        }
        logger.info(f"Live data for {ticker}: {json.dumps(live_data, default=str)}")
        return live_data
    except Exception as e:
        logger.error(f"Error fetching live data for {ticker}: {e}")
        return {}

def fetch_frequent_data(ticker):
    """
    Fetch frequently updated stock data for a given ticker using yahooquery.
    
    Args:
    - ticker (str): Stock ticker symbol.
    
    Returns:
    - dict: Frequently updated stock data.
    """
    try:
        stock = Ticker(ticker)
        summary_detail = stock.summary_detail.get(ticker, {})
        frequent_data = {
            'previous_close': summary_detail.get('previousClose'),
            'open': summary_detail.get('open'),
            'day_high': summary_detail.get('dayHigh'),
            'day_low': summary_detail.get('dayLow'),
            'volume': summary_detail.get('volume'),
            'timestamp': datetime.datetime.now().isoformat()
        }
        logger.info(f"Frequent data for {ticker}: {json.dumps(frequent_data, default=str)}")
        return frequent_data
    except Exception as e:
        logger.error(f"Error fetching frequent data for {ticker}: {e}")
        return {}

def fetch_infrequent_data(ticker):
    """
    Fetch infrequently updated stock data for a given ticker using yahooquery.
    
    Args:
    - ticker (str): Stock ticker symbol.
    
    Returns:
    - dict: Infrequently updated stock data.
    """
    try:
        stock = Ticker(ticker)
        summary_profile = stock.summary_profile.get(ticker, {})
        infrequent_data = {
            'sector': summary_profile.get('sector'),
            'industry': summary_profile.get('industry'),
            'full_time_employees': summary_profile.get('fullTimeEmployees'),
            'timestamp': datetime.datetime.now().isoformat()
        }
        logger.info(f"Infrequent data for {ticker}: {json.dumps(infrequent_data, default=str)}")
        return infrequent_data
    except Exception as e:
        logger.error(f"Error fetching infrequent data for {ticker}: {e}")
        return {}

def fetch_general_data(ticker):
    """
    Fetch general stock data for a given ticker using yahooquery.
    
    Args:
    - ticker (str): Stock ticker symbol.
    
    Returns:
    - dict: General stock data.
    """
    try:
        stock = Ticker(ticker)
        asset_profile = stock.asset_profile.get(ticker, {})
        general_data = {
            'long_business_summary': asset_profile.get('longBusinessSummary'),
            'country': asset_profile.get('country'),
            'website': asset_profile.get('website'),
            'timestamp': datetime.datetime.now().isoformat()
        }
        # log output with general info but no dump bc it is too long
        logger.info(f"General data for {ticker}")
        return general_data
    except Exception as e:
        logger.error(f"Error fetching general data for {ticker}: {e}")
        return {}

def save_data(ticker, data):
    """
    Save stock data to a JSON file.
    
    Args:
    - ticker (str): Stock ticker symbol.
    - data (dict): Stock data to save.
    """
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r') as file:
                all_data = json.load(file)
        else:
            all_data = {}

        all_data[ticker] = data

        with open(DATA_FILE, 'w') as file:
            json.dump(all_data, file, indent=4)
        logger.info(f"Data saved for {ticker}")
    except Exception as e:
        logger.error(f"Error saving data for {ticker}: {e}")

def main():
    logger.info("...Starting data collection...")
    for ticker in ticker_list:
        logger.info(f"Collecting data for {ticker}")
        live_data = fetch_live_data(ticker)
        frequent_data = fetch_frequent_data(ticker)
        infrequent_data = fetch_infrequent_data(ticker)
        general_data = fetch_general_data(ticker)

        combined_data = {
            'live_data': live_data,
            'frequent_data': frequent_data,
            'infrequent_data': infrequent_data,
            'general_data': general_data
        }

        save_data(ticker, combined_data)
    logger.info("...Data collection complete...")

if __name__ == "__main__":
    main()
    
    
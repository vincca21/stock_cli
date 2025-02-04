#yfin_working.py

"""
Description:
- Practice using yfinance to get stock data
    - Get stock data for a specific date range, particularly most recent data possible
- figure all data received from yfinance and how to use it to plan database schema
"""
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))
import datetime

import yfinance as yf

from logs.logging import get_logger

# Set up logging
logger = get_logger()


# get most recent data for a stock
def get_recent_data(ticker):
    logger.info(f"Getting recent data for {ticker}")
    
    # get up to date data -> set time to now
    now = datetime.datetime.now()
    
    # fetch data
    data = yf.download(ticker, start=now, end=now)
    
    # check if data is empty
    if data.empty:
        logger.error(f"No data found for {ticker}")
        return None
    
    # Remove redundant ticker name from column headers
    data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    # Format and log stock data
    formatted_data = data.reset_index()
    formatted_data_str = formatted_data.to_string(index=False)
    
    logger.info(f"\n=== {ticker} Data ===\n{formatted_data_str}\n")
    return formatted_data


def main():
    test_tickers = ['AAPL', 'MSFT', 'TSLA']
    for ticker in test_tickers:
        get_recent_data(ticker)
    logger.info("End of test run")
    
if __name__ == "__main__":
    logger.info("Starting test run")
    main()
    logger.info("End of test run")

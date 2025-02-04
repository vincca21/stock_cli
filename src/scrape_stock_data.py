# scrape_stock_data.py

import os
import sys
import datetime
import json
import sqlite3
from yahooquery import Ticker
from logs.logging import get_logger

logger = get_logger()

# Database setup
DB_FILE = "data/stock_data.db"

def create_tables():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS live_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT,
        price REAL,
        change REAL,
        percent_change REAL,
        timestamp TEXT
    )
    """)
    conn.commit()
    conn.close()

def fetch_live_data(ticker):
    logger.info(f"Fetching live data for {ticker}")
    try:
        stock = Ticker(ticker)
        market_data = stock.price.get(ticker, {})
        live_data = {
            'ticker': ticker,
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

def save_data(data):
    try:
        logger.info(f"Saving data for {data['ticker']}")
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO live_data (ticker, price, change, percent_change, timestamp)
        VALUES (?, ?, ?, ?, ?)
        """, (data['ticker'], data['price'], data['change'], data['percent_change'], data['timestamp']))
        conn.commit()
        conn.close()
        logger.info(f"Data saved for {data['ticker']}")
    except Exception as e:
        logger.error(f"Error saving data for {data['ticker']}: {e}")

def main():
    create_tables()
    ticker_list = ['AAPL', 'GOOGL', 'AMZN']
    logger.info("...Starting data collection...")
    for ticker in ticker_list:
        live_data = fetch_live_data(ticker)
        if live_data:
            save_data(live_data)
    logger.info("...Data collection complete...")

if __name__ == "__main__":
    main()
import sqlite3
import time
from logs.logging import get_logger

# Import your scraping functions
from scrape import (
    fetch_live_data, 
    fetch_daily_data, 
    fetch_fundamental_data, 
    fetch_analysis_data,
)

logger = get_logger()

# -----------------------------
# Database Setup
# -----------------------------

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS Ticker (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS LiveData (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER,
    price REAL,
    change REAL,
    percent_change REAL,
    timestamp TEXT,
    FOREIGN KEY (ticker_id) REFERENCES Ticker(id)
);

CREATE TABLE IF NOT EXISTS DailyData (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER,
    open REAL,
    previous_close REAL,
    day_high REAL,
    day_low REAL,
    volume INTEGER,
    market_cap INTEGER,
    trailing_pe REAL,
    forward_pe REAL,
    timestamp TEXT,
    FOREIGN KEY (ticker_id) REFERENCES Ticker(id)
);

CREATE TABLE IF NOT EXISTS Fundamentals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER,
    sector TEXT,
    industry TEXT,
    full_time_employees INTEGER,
    country TEXT,
    website TEXT,
    description TEXT,
    timestamp TEXT,
    FOREIGN KEY (ticker_id) REFERENCES Ticker(id)
);

CREATE TABLE IF NOT EXISTS Analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER,
    computed_recommendation TEXT,
    pe_ratio REAL,
    peg_ratio REAL,
    next_quarter_growth REAL,
    timestamp TEXT,
    FOREIGN KEY (ticker_id) REFERENCES Ticker(id)
);

CREATE TABLE IF NOT EXISTS RecommendationTrend (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id INTEGER,
    period TEXT,
    strongBuy INTEGER,
    buy INTEGER,
    hold INTEGER,
    sell INTEGER,
    strongSell INTEGER,
    FOREIGN KEY (analysis_id) REFERENCES Analysis(id)
);

CREATE TABLE IF NOT EXISTS EarningsTrend (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id INTEGER,
    period TEXT,
    growth REAL,
    FOREIGN KEY (analysis_id) REFERENCES Analysis(id)
);

CREATE TABLE IF NOT EXISTS IndexTrend (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id INTEGER,
    peRatio REAL,
    pegRatio REAL,
    FOREIGN KEY (analysis_id) REFERENCES Analysis(id)
);
"""


def init_db(db_path: str):
    """
    Initialize the SQLite database with the required schema.
    Returns a connection object.
    """
    logger.info(f"Initializing database at {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.executescript(CREATE_TABLES_SQL)
    conn.commit()
    return conn


# -----------------------------
# Helper Functions for Inserts
# -----------------------------

def get_or_create_ticker_id(conn, symbol):
    """
    Upsert a ticker symbol into Ticker table and return its id.
    """
    # Insert the ticker symbol if it doesn't exist, else ignore
    insert_sql = """
        INSERT OR IGNORE INTO Ticker (symbol)
        VALUES (?)
    """
    cursor = conn.cursor()
    cursor.execute(insert_sql, (symbol,))
    conn.commit()

    # Retrieve the ticker_id
    select_sql = "SELECT id FROM Ticker WHERE symbol = ?"
    cursor.execute(select_sql, (symbol,))
    row = cursor.fetchone()
    return row[0] if row else None


def store_live_data(conn, ticker_id, data):
    """
    Insert a new row into LiveData table.
    `data` should be a dict like: {
       'price': ...,
       'change': ...,
       'percent_change': ...,
       'timestamp': ...
    }
    """
    insert_sql = """
        INSERT INTO LiveData 
            (ticker_id, price, change, percent_change, timestamp)
        VALUES 
            (?, ?, ?, ?, ?)
    """
    cursor = conn.cursor()
    cursor.execute(insert_sql, (
        ticker_id,
        data.get('price'),
        data.get('change'),
        data.get('percent_change'),
        data.get('timestamp')
    ))
    conn.commit()


def store_daily_data(conn, ticker_id, data):
    """
    Insert a new row into DailyData table.
    """
    insert_sql = """
        INSERT INTO DailyData 
            (ticker_id, open, previous_close, day_high, day_low, volume,
             market_cap, trailing_pe, forward_pe, timestamp)
        VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor = conn.cursor()
    cursor.execute(insert_sql, (
        ticker_id,
        data.get('open'),
        data.get('previous_close'),
        data.get('day_high'),
        data.get('day_low'),
        data.get('volume'),
        data.get('market_cap'),
        data.get('trailing_pe'),
        data.get('forward_pe'),
        data.get('timestamp')
    ))
    conn.commit()


def store_fundamental_data(conn, ticker_id, data):
    """
    Insert a new row into Fundamentals table.
    """
    insert_sql = """
        INSERT INTO Fundamentals 
            (ticker_id, sector, industry, full_time_employees, country, 
             website, description, timestamp)
        VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor = conn.cursor()
    cursor.execute(insert_sql, (
        ticker_id,
        data.get('sector'),
        data.get('industry'),
        data.get('full_time_employees'),
        data.get('country'),
        data.get('website'),
        data.get('description'),
        data.get('timestamp')
    ))
    conn.commit()


def store_analysis_data(conn, ticker_id, data):
    """
    Insert data into Analysis table, then store 
    recommendation_trend, earnings_trend, index_trend in separate tables.
    """
    analysis_dict = data.get('analysis', {})
    summary = analysis_dict.get('summary', {})
    full_data = analysis_dict.get('full_data', {})
    timestamp = analysis_dict.get('timestamp')

    insert_analysis_sql = """
        INSERT INTO Analysis 
            (ticker_id, computed_recommendation, pe_ratio, peg_ratio, 
             next_quarter_growth, timestamp)
        VALUES 
            (?, ?, ?, ?, ?, ?)
    """
    cursor = conn.cursor()
    cursor.execute(insert_analysis_sql, (
        ticker_id,
        summary.get('recommendation'),
        summary.get('pe_ratio'),
        summary.get('peg_ratio'),
        summary.get('next_quarter_growth'),
        timestamp
    ))
    conn.commit()

    analysis_id = cursor.lastrowid  # capture the newly inserted Analysis id

    # Store recommendation trend
    recommendation_list = full_data.get('recommendation_trend', [])
    insert_recommend_sql = """
        INSERT INTO RecommendationTrend
            (analysis_id, period, strongBuy, buy, hold, sell, strongSell)
        VALUES
            (?, ?, ?, ?, ?, ?, ?)
    """
    for rec_row in recommendation_list:
        cursor.execute(insert_recommend_sql, (
            analysis_id,
            rec_row.get('period'),
            rec_row.get('strongBuy'),
            rec_row.get('buy'),
            rec_row.get('hold'),
            rec_row.get('sell'),
            rec_row.get('strongSell'),
        ))

    # Store earnings trend
    earnings_dict = full_data.get('earnings_trend', {})
    trend_list = earnings_dict.get('trend', [])
    insert_earnings_sql = """
        INSERT INTO EarningsTrend
            (analysis_id, period, growth)
        VALUES
            (?, ?, ?)
    """
    for e_row in trend_list:
        cursor.execute(insert_earnings_sql, (
            analysis_id,
            e_row.get('period'),
            e_row.get('growth'),
        ))

    # Store index trend
    index_dict = full_data.get('index_trend', {})
    insert_index_sql = """
        INSERT INTO IndexTrend
            (analysis_id, peRatio, pegRatio)
        VALUES
            (?, ?, ?)
    """
    if index_dict:
        cursor.execute(insert_index_sql, (
            analysis_id,
            index_dict.get('peRatio'),
            index_dict.get('pegRatio'),
        ))
    conn.commit()

# -----------------------------
# New function to fetch/store live data for 1 ticker
# -----------------------------

def fetch_and_store_live_for_ticker(db_path, ticker):
    """
    Fetch *only* the live data for a single ticker, then store in DB.
    This helps the CLI quickly refresh a single ticker's live data.
    """
    logger.info(f"Fetching live data for single ticker '{ticker}'")
    conn = init_db(db_path)
    ticker_data = fetch_live_data([ticker])  # returns a dict { ticker: {...} }
    
    if ticker in ticker_data:
        # find or create the Ticker row
        t_id = get_or_create_ticker_id(conn, ticker)
        # store the live data
        store_live_data(conn, t_id, ticker_data[ticker])
        logger.info(f"Stored latest live data for '{ticker}' in DB.")
    else:
        logger.warning(f"No live data returned for '{ticker}'")
    
    conn.close()

# -----------------------------
# Main Execution
# -----------------------------

def main():
    """
    Demonstration of:
      - Creating/connecting to a DB
      - Fetching data (in bulk) for a large list of tickers
      - Storing data into the DB
    """
    logger.info("Starting DB ingest process...")
    # Example large list of tickers
    ticker_list = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM",
        "V", "WMT", "PG", "DIS", "BAC", "XOM", "HD", "INTC", "MA", "GE",
        "PFE", "KO", "PEP", "CSCO", "ORCL", "NFLX", "ADBE"
        # ... potentially many more
    ]

    db_path = "data/stock_data.db"
    conn = init_db(db_path)

    start_time = time.time()

    # 1) Fetch Data
    logger.info(f"Fetching data for {len(ticker_list)} tickers.")
    live_data = fetch_live_data(ticker_list)
    daily_data = fetch_daily_data(ticker_list)
    fundamental_data = fetch_fundamental_data(ticker_list)
    analysis_data = fetch_analysis_data(ticker_list)

    # 2) For each ticker, insert all data
    #    NOTE: In a production scenario, you might wrap the entire loop in a 
    #    transaction for better performance with large data sets.
    all_tickers = set(ticker_list)
    for tkr in all_tickers:
        logger.info(f"Storing data for ticker='{tkr}' in DB.")
        ticker_id = get_or_create_ticker_id(conn, tkr)
        
        # live_data[tkr] might not exist if an error occurred
        if tkr in live_data:
            store_live_data(conn, ticker_id, live_data[tkr])
        
        if tkr in daily_data:
            store_daily_data(conn, ticker_id, daily_data[tkr])
        
        if tkr in fundamental_data:
            store_fundamental_data(conn, ticker_id, fundamental_data[tkr])
        
        if tkr in analysis_data:
            store_analysis_data(conn, ticker_id, analysis_data[tkr])

    total_time = time.time() - start_time
    logger.info(f"Data ingestion completed in {total_time:.2f}s")

    conn.close()
    logger.info("Database connection closed. Exiting...")


if __name__ == "__main__":
    main()

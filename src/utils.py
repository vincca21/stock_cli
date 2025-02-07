# utils.py

import os
import sqlite3
from logs.logging import get_logger

logger = get_logger()

# ---------------------------------------------------------
# SHARED SETTINGS
# ---------------------------------------------------------

# Basic Ticker List for testing or small runs
TICKER_LIST = [
    "AAPL", 
    "MSFT", 
    "GOOGL", 
    "AMZN"
    # Add more if needed
]

# Database file name (shared path for all scripts)
DB_FILE_NAME = "stock_data_testing.db"
DB_FILE = os.path.join("data", DB_FILE_NAME)

# Interval (in seconds) for recurring live data updates
LIVE_UPDATE_INTERVAL_SECONDS = 10

# If multiple scripts require the same create-tables SQL, put it here:
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

# ---------------------------------------------------------
# SHARED DB FUNCTIONS (OPTIONAL)
# ---------------------------------------------------------

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


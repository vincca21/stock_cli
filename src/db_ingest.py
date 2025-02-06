# db_ingest.py

import time

# Import the scraping functions from scrape.py
from scrape import (
    fetch_live_data,
    fetch_daily_data,
    fetch_fundamental_data,
    fetch_analysis_data
)

# Import shared constants and functions from utils.py
from utils import (
    logger,             # We can reuse the same logger as in run_all.py if desired
    DB_FILE,            # The shared DB file path
    TICKER_LIST,        # The default ticker list for large/batch fetches
    init_db             # Function to initialize the DB with CREATE statements
)

# ---------------------------------------------------------
# Helper Functions for DB Inserts
# ---------------------------------------------------------

def get_or_create_ticker_id(conn, symbol):
    """
    Upsert a ticker symbol into Ticker table and return its id.
    """
    insert_sql = """
        INSERT OR IGNORE INTO Ticker (symbol)
        VALUES (?)
    """
    cursor = conn.cursor()
    cursor.execute(insert_sql, (symbol,))
    conn.commit()

    select_sql = "SELECT id FROM Ticker WHERE symbol = ?"
    cursor.execute(select_sql, (symbol,))
    row = cursor.fetchone()
    return row[0] if row else None


def store_live_data(conn, ticker_id, data):
    """
    Insert a new row into LiveData table.
    `data` should be a dict like:
        {
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
            (ticker_id, open, previous_close, day_high, day_low,
             volume, market_cap, trailing_pe, forward_pe, timestamp)
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
            (ticker_id, sector, industry, full_time_employees,
             country, website, description, timestamp)
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
    Insert data into Analysis table, then store sub-data in:
        - RecommendationTrend
        - EarningsTrend
        - IndexTrend
    See 'analysis': {
        'full_data': {
            'recommendation_trend': [...],
            'computed_recommendation': '...',
            'earnings_trend': {...},
            'index_trend': {...}
        },
        'summary': { ... },
        'timestamp': ...
    }
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

    analysis_id = cursor.lastrowid  # capture the newly inserted Analysis ID

    # Store recommendation trend (list of dicts)
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
            rec_row.get('strongSell')
        ))

    # Store earnings trend (dict that often has 'trend' -> list of dicts)
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

    # Store index trend (a single dict)
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

# ---------------------------------------------------------
# Single-Ticker Live Fetch
# ---------------------------------------------------------

def fetch_and_store_live_for_ticker(db_path, ticker):
    """
    Fetch ONLY live data for the specified ticker, then store in the DB.
    Ideal for CLI 'refresh' usage.
    """
    logger.info(f"Fetching live data for single ticker='{ticker}'")
    conn = init_db(db_path)  # calls CREATE TABLES IF NOT EXISTS, etc.

    # fetch_live_data returns { ticker: {...} }
    ticker_data = fetch_live_data([ticker])
    if ticker not in ticker_data:
        logger.warning(f"No live data returned for {ticker}")
        conn.close()
        return

    # Insert or retrieve Ticker row
    t_id = get_or_create_ticker_id(conn, ticker)
    # Insert into LiveData
    store_live_data(conn, t_id, ticker_data[ticker])
    logger.info(f"Stored latest live data for '{ticker}' in DB.")

    conn.close()

# ---------------------------------------------------------
# Main Execution for Bulk Ingestion
# ---------------------------------------------------------

def main():
    """
    Demonstration of:
      - Initializing the DB
      - Fetching data (in bulk) for TICKER_LIST
      - Storing each category in the DB
    """
    logger.info("Starting DB Ingest Process...")

    conn = init_db(DB_FILE)  # Ensures tables exist

    start_time = time.time()
    logger.info(f"Fetching data for {len(TICKER_LIST)} tickers...")

    live_data = fetch_live_data(TICKER_LIST)
    daily_data = fetch_daily_data(TICKER_LIST)
    fundamental_data = fetch_fundamental_data(TICKER_LIST)
    analysis_data = fetch_analysis_data(TICKER_LIST)

    # Bulk insert
    for symbol in TICKER_LIST:
        logger.info(f"Storing data for ticker='{symbol}' in DB.")
        t_id = get_or_create_ticker_id(conn, symbol)

        # Live
        if symbol in live_data:
            store_live_data(conn, t_id, live_data[symbol])

        # Daily
        if symbol in daily_data:
            store_daily_data(conn, t_id, daily_data[symbol])

        # Fundamentals
        if symbol in fundamental_data:
            store_fundamental_data(conn, t_id, fundamental_data[symbol])

        # Analysis
        if symbol in analysis_data:
            store_analysis_data(conn, t_id, analysis_data[symbol])

    total_time = time.time() - start_time
    logger.info(f"Data ingestion completed in {total_time:.2f}s")

    conn.close()
    logger.info("Database connection closed. Exiting db_ingest.py...")


if __name__ == "__main__":
    main()

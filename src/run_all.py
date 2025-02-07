# run_all.py

import signal
import sqlite3
import subprocess
import sys
import threading
import time

import schedule

from cli import display_basic_data_for_all_tickers
from logs.logging import get_logger

# Import from utils
from utils import DB_FILE, LIVE_UPDATE_INTERVAL_SECONDS  # TICKER_LIST,

logger = get_logger()

# Also import your other code or references here.
# For example: from utils import init_db, CREATE_TABLES_SQL, etc.


# ---------------------------------------------------------
# One-time Full Ingestion
# ---------------------------------------------------------

DB_INGEST_SCRIPT = "src/db_ingest.py"
CLI_SCRIPT = "src/cli.py"

def run_full_ingest():
    logger.info("Starting full data ingestion (all categories)...")
    try:
        subprocess.run([sys.executable, DB_INGEST_SCRIPT], check=True)
        logger.info("Full data ingestion completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Full data ingestion failed: {e}")


# ---------------------------------------------------------
# Display Basic Data for All Tickers
# ---------------------------------------------------------

def display_basic_data_for_all_tickers():
    """
    Example reading from DB_FILE from utils
    """
    logger.info("Displaying a summary of basic data for all tickers.")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        query = """
        SELECT t.symbol, l.price, l.change, l.percent_change, l.timestamp
        FROM LiveData l
        JOIN Ticker t ON t.id = l.ticker_id
        WHERE l.id IN (
            SELECT MAX(id) 
            FROM LiveData 
            GROUP BY ticker_id
        )
        ORDER BY t.symbol
        """
        rows = cursor.execute(query).fetchall()
        conn.close()

        if not rows:
            print("No live data found in the DB.")
            return

        print("\n=== LIVE DATA SNAPSHOT ===")
        print(f"{'Symbol':<8} {'Price':>10} {'Change':>10} {'PctChg':>10} {'Timestamp':>25}")
        print("-" * 70)
        for (symbol, price, change, pct, ts) in rows:
            print(f"{symbol:<8} {price:>10.2f} {change:>10.2f} {pct:>10.2f} {ts:>25}")
        print("-" * 70)
    except Exception as e:
        logger.error(f"Error displaying basic data: {e}")
        print("[ERROR] Unable to display basic data for tickers.\n")



# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    logger.info("run_all.py started. Press Ctrl+C to stop.")

    # Step 1) Full ingestion with a spinner
    show_spinner(run_full_ingest)

    # Step 2) Schedule repeated updates
    logger.info(f"Scheduling live data updates every {LIVE_UPDATE_INTERVAL_SECONDS} minutes.")
    schedule.every(LIVE_UPDATE_INTERVAL_SECONDS).minutes.do(run_live_data_update)
    sched_thread = threading.Thread(target=schedule_runner, daemon=True)
    sched_thread.start()

    # Step 3) Display the data that was just collected
    display_basic_data_for_all_tickers()

    print("----------------------------------------------------")
    print("Available CLI commands (in the separate Terminal):")
    print("  live <TICKER> --refresh   # Refresh & show live data for a ticker")
    print("  live <TICKER>             # Show existing live data for a ticker")
    print("----------------------------------------------------")


    # Step 4) Keep the script alive
    try:
        while True:
            show_countdown_to_next_update(LIVE_UPDATE_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Graceful shutdown requested. Exiting run_all.py.")

if __name__ == "__main__":
    def handle_signal(sig, frame):
        logger.info(f"Received signal {sig}, exiting run_all.py...")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    main()

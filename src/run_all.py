# run_all.py

import os
import time
import schedule
import subprocess
import sys
import threading
import signal
import sqlite3
from logs.logging import get_logger

# Import from utils
from utils import (
    #TICKER_LIST,
    DB_FILE,
    LIVE_UPDATE_INTERVAL_MINUTES
)

logger = get_logger()

# Also import your other code or references here.
# For example: from utils import init_db, CREATE_TABLES_SQL, etc.

# ---------------------------------------------------------
# Spinner Utility
# ---------------------------------------------------------

complete = [False]  # shared variable to signal spinner to stop

def show_spinner(function_name, *args, **kwargs):
    """
    Show a spinner while a long-running function is in progress.
    """
    def spinner_task():
        spinner_chars = ["|", "/", "-", "\\"]
        i = 0
        while not complete[0]:
            sys.stdout.write(f"\rAccessing & Loading Data... {spinner_chars[i]} ")
            sys.stdout.flush()
            i = (i + 1) % len(spinner_chars)
            time.sleep(0.1)
        sys.stdout.write("\rData Loading Completed...  \n")

    spinner_thread = threading.Thread(target=spinner_task, daemon=True)
    spinner_thread.start()

    function_name(*args, **kwargs)

    complete[0] = True
    spinner_thread.join()

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
# Recurring Live Data Updates
# ---------------------------------------------------------

def run_live_data_update():
    logger.info("Starting recurring live data update...")
    try:
        subprocess.run([sys.executable, DB_INGEST_SCRIPT], check=True)
        logger.info("Live data update completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Live data update failed: {e}")

# ---------------------------------------------------------
# Scheduling Thread
# ---------------------------------------------------------

def schedule_runner():
    while True:
        schedule.run_pending()
        time.sleep(1)

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

        print("\n=== BASIC LIVE DATA SNAPSHOT ===")
        print(f"{'Symbol':<8} {'Price':>10} {'Change':>10} {'PctChg':>10} {'Timestamp':>25}")
        print("-" * 70)
        for (symbol, price, change, pct, ts) in rows:
            print(f"{symbol:<8} {price:>10.2f} {change:>10.2f} {pct:>10.2f} {ts:>25}")
        print("-" * 70)
        print("End of snapshot.\n")
    except Exception as e:
        logger.error(f"Error displaying basic data: {e}")
        print("[ERROR] Unable to display basic data for tickers.\n")

# ---------------------------------------------------------
# Countdown
# ---------------------------------------------------------

def show_countdown_to_next_update(interval_minutes):
    logger.info("Showing countdown to next update.")
    seconds_remaining = interval_minutes * 60
    while seconds_remaining > 0:
        mins, secs = divmod(seconds_remaining, 60)
        sys.stdout.write(f"\rNext auto-update in: {mins:02d}:{secs:02d} ")
        sys.stdout.flush()
        time.sleep(1)
        seconds_remaining -= 1
    sys.stdout.write("\n")

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    logger.info("run_all.py started. Press Ctrl+C to stop.")

    # Step 1) Full ingestion with a spinner
    show_spinner(run_full_ingest)

    # Step 2) Schedule repeated updates
    logger.info(f"Scheduling live data updates every {LIVE_UPDATE_INTERVAL_MINUTES} minutes.")
    schedule.every(LIVE_UPDATE_INTERVAL_MINUTES).minutes.do(run_live_data_update)
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
            show_countdown_to_next_update(LIVE_UPDATE_INTERVAL_MINUTES)
    except KeyboardInterrupt:
        logger.info("Graceful shutdown requested. Exiting run_all.py.")

if __name__ == "__main__":
    def handle_signal(sig, frame):
        logger.info(f"Received signal {sig}, exiting run_all.py...")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    main()

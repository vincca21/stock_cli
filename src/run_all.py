# run_all.py

import os
import time
import schedule
import subprocess
import sys
import threading
import signal
import sqlite3
import platform

from logs.logging import get_logger

logger = get_logger()

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

# You can easily adjust these for tuning/experimentation
TICKER_LIST = [
    "AAPL", "MSFT", "GOOGL", "AMZN"] 

""", "TSLA", "META",
    "NVDA", "JPM", "V", "WMT", "PG", "DIS", "BAC",
    "XOM", "HD", "INTC", "MA", "GE", "PFE", "KO",
    "PEP", "CSCO", "ORCL", "NFLX", "ADBE"
]
"""

# How often we update live data (in minutes)
LIVE_UPDATE_INTERVAL_MINUTES = 1

# Paths to our scripts
DB_INGEST_SCRIPT = "src/db_ingest.py"
CLI_SCRIPT = "src/cli.py"

DB_FILE = os.path.join("data", "stock_data_testing.db")

logger.info("Importing run_all.py Configuration...")
# ---------------------------------------------------------
# Spinner Utility - Progress Indicator
# ---------------------------------------------------------

def show_spinner(function_name, *args, **kwargs):
    """
    Show a spinner while a long-running function is in progress.
    """
    complete = False # mutable sentinel: a list to hold a boolean to indicate completion
    
    def spinner_task():
        spinner_chars = ["|", "/", "-", "\\"] # spinner animation characters
        i = 0
        while not complete[0]:
            sys.stdout.write(f"\rAccessing & Loading Data... {spinner_chars[i]} ")
            sys.stdout.flush() # force the console to update
            i = (i + 1) % len(spinner_chars) # cycle through the spinner characters
            time.sleep(0.1) # wait 100ms between updates
        sys.stdout.write("\rData Loading Completed...  \n") # clear the spinner
        
    spinner_thread = threading.Thread(target=spinner_task, daemon=True)
    spinner_thread.start() # start the spinner
    
    # Call the function with the provided arguments
    try:
        function_name(*args, **kwargs)
    except Exception as e:
        complete[0] = True
        raise e
    finally:
        complete[0] = True # signal the spinner to stop
        spinner_thread.join() # wait for the spinner to stop
            
# ---------------------------------------------------------
# One-time Full Ingestion
# ---------------------------------------------------------

def run_full_ingest():
    """
    Run the DB ingestion script which fetches and stores:
      - live data
      - daily data
      - fundamental data
      - analysis data
    for all tickers.
    """
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
    """
    If you have a specialized "store_live_data_only" approach or
    "fetch_and_store_live_for_ticker" in db_ingest.py, you can call it here.
    For simplicity, we re-run the entire DB ingest, but that’s optional.
    """
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
    """
    Runs pending scheduled jobs in a loop.
    This function is meant to run on a background thread so the main thread
    can handle user interaction (CLI).
    """
    while True:
        schedule.run_pending()
        time.sleep(1)

# ---------------------------------------------------------
# Launch CLI in a New Terminal (macOS)
# ---------------------------------------------------------

def launch_cli_in_new_terminal():
    """
    On macOS, we'll use osascript to open a new Terminal tab/window
    and run our CLI script inside it.
    
    This AppleScript command tells Terminal to open a new window
    and run: `python3 src/cli.py`
    """
    logger.info("Attempting to launch CLI in a new Terminal window on macOS...")

    # We create a small AppleScript snippet that instructs Terminal to:
    # 1) Open a new window
    # 2) Run our Python CLI script
    cmd_script = (
        f'tell application "Terminal" to do script '
        f'"{sys.executable} {CLI_SCRIPT}"'
    )

    try:
        subprocess.run(["osascript", "-e", cmd_script], check=True)
        logger.info("Successfully launched CLI in new Terminal window.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to launch CLI in new Terminal: {e}")
        # Fallback: run CLI in the same window
        subprocess.run([sys.executable, CLI_SCRIPT], check=True)

# ---------------------------------------------------------
# Display Basic Data for All Tickers
# ---------------------------------------------------------

def display_basic_data_for_all_tickers():
    """
    Queries the DB for a simple snapshot of each ticker's live data
    and prints it in a table-like format.
    """
    logger.info("Displaying a summary of basic data for all tickers.")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # We'll get the most recent LiveData entry per ticker
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
# Countdown to Next Update
# ---------------------------------------------------------

def show_countdown_to_next_update(interval_minutes):
    """
    Displays a simple countdown to the next live data update.
    This is purely cosmetic; once done, it returns control.
    """
    logger.info("Showing countdown to next update.")
    seconds_remaining = interval_minutes * 60
    while seconds_remaining > 0:
        mins, secs = divmod(seconds_remaining, 60)
        sys.stdout.write(f"\rNext auto-update in: {mins:02d}:{secs:02d} ")
        sys.stdout.flush()
        time.sleep(1)
        seconds_remaining -= 1
    sys.stdout.write("\n")  # move to next line

# ---------------------------------------------------------
# Main Orchestration
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

    # Provide instructions
    print("----------------------------------------------------")
    print("Available CLI commands (in the separate Terminal):")
    print("  live <TICKER> --refresh   # Refresh & show live data for a ticker")
    print("  live <TICKER>             # Show existing live data for a ticker")
    print("----------------------------------------------------")

    # Step 4) Launch the CLI in a separate macOS Terminal window
    launch_cli_in_new_terminal()

    # Keep our main program alive so the schedule keeps running
    # with repeated countdown intervals, or break out if the user hits Ctrl+C.
    try:
        while True:
            show_countdown_to_next_update(LIVE_UPDATE_INTERVAL_MINUTES)
            # The background thread handles the actual update at the scheduled time.
            # After the countdown, we simply loop again.
    except KeyboardInterrupt:
        logger.info("Graceful shutdown requested. Exiting run_all.py.")

if __name__ == "__main__":
    def handle_signal(sig, frame):
        logger.info(f"Received signal {sig}, exiting run_all.py...")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    main()

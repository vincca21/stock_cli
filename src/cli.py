# cli.py

import os
import sys
import time
import subprocess
import sqlite3
import click
import threading
import schedule
from utils import DB_FILE, LIVE_UPDATE_INTERVAL_SECONDS, TICKER_LIST
from logs.logging import get_logger

# Import from db_ingest or utils
# to fetch single ticker data
from db_ingest import fetch_and_store_live_for_ticker

# Import the shared DB_FILE
from utils import DB_FILE

DB_INGEST_SCRIPT = "src/db_ingest.py"

logger = get_logger()

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
# Countdown
# ---------------------------------------------------------

def show_countdown_to_next_update(interval_minutes):
    logger.info("Showing countdown to next update.")
    seconds_remaining = LIVE_UPDATE_INTERVAL_SECONDS
    while seconds_remaining > 0:
        sys.stdout.write(f"\rNext update in {seconds_remaining} seconds... ")
        sys.stdout.flush()
        time.sleep(1)
        seconds_remaining -= 1
    sys.stdout.write("\rUpdating data now...    \n")
    

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


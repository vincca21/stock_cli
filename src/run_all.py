# run_all.py

import time
import schedule
import subprocess
import sys

from logs.logging import get_logger

logger = get_logger()

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

# You can easily adjust these for tuning/experimentation
TICKER_LIST = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META",
    "NVDA", "JPM", "V", "WMT", "PG", "DIS", "BAC",
    "XOM", "HD", "INTC", "MA", "GE", "PFE", "KO",
    "PEP", "CSCO", "ORCL", "NFLX", "ADBE"
]

# How often we update live data (in minutes)
LIVE_UPDATE_INTERVAL_MINUTES = 5

# Path to our DB ingest script or function
DB_INGEST_SCRIPT = "src/db_ingest.py"


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
        # We can call db_ingest.py via subprocess
        subprocess.run([sys.executable, DB_INGEST_SCRIPT], check=True)
        logger.info("Full data ingestion completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Full data ingestion failed: {e}")

# ---------------------------------------------------------
# Recurring Live Data Updates
# ---------------------------------------------------------

def run_live_data_update():
    """
    Demonstrates how to run only live-data updates repeatedly.
    This example calls a hypothetical 'store_live_data_only' function in db_ingest.py via subprocess.
    
    Alternatively, you might import a function from db_ingest and call it directly.
    """
    logger.info("Starting recurring live data update...")
    # Option A: If we have a dedicated script or function:
    #   subprocess.run([sys.executable, "src/db_ingest.py", "--live-only"], check=True)
    
    # Option B (if we had a direct function import):
    #   from db_ingest import store_live_data_only
    #   store_live_data_only(TICKER_LIST)
    
    # For example purposes, we call the entire DB ingest script again:
    # (But ideally, you'd have a specialized function that only updates LiveData.)
    try:
        subprocess.run([sys.executable, DB_INGEST_SCRIPT], check=True)
        logger.info("Live data update completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Live data update failed: {e}")

# ---------------------------------------------------------
# Main Orchestration
# ---------------------------------------------------------

def main():
    """
    1. Perform a full data ingestion once (live, daily, fundamentals, analysis).
    2. Schedule recurring live data updates at configured interval.
    3. Keep the script running to maintain the schedule.
    """
    logger.info("run_all.py started.")
    
    # 1) Do a full ingestion for all categories
    run_full_ingest()
    
    # 2) Schedule repeated updates for live data
    logger.info(f"Scheduling live data updates every {LIVE_UPDATE_INTERVAL_MINUTES} minutes.")
    schedule.every(LIVE_UPDATE_INTERVAL_MINUTES).minutes.do(run_live_data_update)
    
    # 3) Loop forever to maintain the schedule
    logger.info("Entering scheduled loop. Press Ctrl+C to stop.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Graceful shutdown requested. Exiting run_all.py...")

if __name__ == "__main__":
    main()
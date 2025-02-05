import time
import schedule
import subprocess
import sys
import threading
import signal

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

# Paths to our scripts
DB_INGEST_SCRIPT = "src/db_ingest.py"
CLI_SCRIPT = "src/cli.py"

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
# CLI Runner
# ---------------------------------------------------------

def run_cli():
    """
    Run the CLI in the foreground, allowing user commands while the
    background schedule keeps running.
    """
    logger.info("Launching CLI in interactive mode. Press CTRL+C to exit.")
    try:
        subprocess.run([sys.executable, CLI_SCRIPT], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"CLI encountered an error: {e}")
    except KeyboardInterrupt:
        logger.info("CLI interrupted by user.")

# ---------------------------------------------------------
# Main Orchestration
# ---------------------------------------------------------

def main():
    """
    1. Perform a full data ingestion once (live, daily, fundamentals, analysis).
    2. Schedule recurring live data updates at configured interval on a background thread.
    3. Launch the CLI in the main thread for interactive commands.
    4. Gracefully shutdown when the user presses CTRL+C on the CLI.
    """
    logger.info("run_all.py started.")
    
    # 1) Full ingestion for all categories
    run_full_ingest()
    
    # 2) Schedule repeated updates for live data
    logger.info(f"Scheduling live data updates every {LIVE_UPDATE_INTERVAL_MINUTES} minutes.")
    schedule.every(LIVE_UPDATE_INTERVAL_MINUTES).minutes.do(run_live_data_update)
    
    # Start the scheduling loop in a separate daemon thread
    sched_thread = threading.Thread(target=schedule_runner, daemon=True)
    sched_thread.start()

    # 3) Run the CLI in the main thread
    run_cli()

    # 4) If CLI exits, we shut down the script
    logger.info("CLI has exited. Stopping run_all.py...")

if __name__ == "__main__":
    # Optional: handle CTRL+C or other signals gracefully in the main process
    def handle_signal(sig, frame):
        logger.info(f"Received signal {sig}, exiting run_all.py...")
        sys.exit(0)
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    main()

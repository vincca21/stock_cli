import subprocess
import sys
from logs.logging import get_logger

logger = get_logger()

def run_scrape():
    logger.info("Starting data scraping...")
    try:
        subprocess.run([sys.executable, "src/scrape_stock_data.py"], check=True)
        logger.info("Data scraping completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Data scraping failed: {e}")

def run_cli():
    logger.info("Starting CLI...")
    try:
        subprocess.run([sys.executable, "src/cli.py"], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"CLI failed: {e}")

if __name__ == "__main__":
    run_scrape()
    run_cli()
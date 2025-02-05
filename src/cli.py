# cli.py

import sqlite3
import click
import subprocess
import sys
from logs.logging import get_logger

logger = get_logger()

DB_FILE = "stock_data.db"  # Adjust path if needed


@click.group()
def cli():
    """A simple command-line interface for viewing stock data."""
    pass


@click.command()
@click.argument('ticker')
@click.option('--refresh/--no-refresh', default=False,
              help="If set, fetch and store the latest live data before displaying.")
def live(ticker, refresh):
    """
    Show the most recent live data for a given TICKER.
    
    Example usage:
      python cli.py live AAPL --refresh
    """
    logger.info(f"Live command called for ticker='{ticker}', refresh={refresh}")

    # 1) Optionally refresh the data
    if refresh:
        logger.info(f"Refreshing live data for {ticker} before displaying.")
        try:
            # Example: call db_ingest.py with a hypothetical --live-only and --ticker CLI
            #   so it fetches only new live data for this single ticker.
            # Adjust this to match your actual db_ingest options or approach.
            subprocess.run([
                sys.executable, 
                "src/db_ingest.py", 
                "--live-only", 
                f"--ticker={ticker}"
            ], check=True)
            logger.info(f"Successfully refreshed live data for {ticker}.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Live data refresh failed for {ticker}: {e}")
            click.echo(f"\n[ERROR] Could not refresh live data for {ticker}.\n")

    # 2) Query the database for the latest entry for this ticker
    logger.info(f"Fetching latest live data from DB for {ticker}")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        query = """
        SELECT t.symbol, l.price, l.change, l.percent_change, l.timestamp
        FROM LiveData l
        JOIN Ticker t ON t.id = l.ticker_id
        WHERE t.symbol = ?
        ORDER BY l.id DESC
        LIMIT 1
        """

        cursor.execute(query, (ticker,))
        data = cursor.fetchone()
        conn.close()

        if data:
            # data = (symbol, price, change, percent_change, timestamp)
            click.echo("\n--- LIVE DATA ---")
            click.echo(f" Ticker:         {data[0]}")
            click.echo(f" Price:          {data[1]}")
            click.echo(f" Change:         {data[2]}")
            click.echo(f" Percent Change: {data[3]}%")
            click.echo(f" Timestamp:      {data[4]}")
            click.echo("---\n")
        else:
            click.echo(f"No live data found in the DB for ticker='{ticker}'.")
    except Exception as e:
        logger.error(f"Error fetching live data for {ticker}: {e}")
        click.echo(f"[ERROR] Unable to retrieve live data for {ticker}")


cli.add_command(live)


if __name__ == "__main__":
    cli()

# cli.py

import sqlite3
import click
import os
from logs.logging import get_logger

# Import the new function to refresh just one ticker
from db_ingest import fetch_and_store_live_for_ticker

logger = get_logger()

# Adjust if your DB is stored elsewhere
DB_FILE = os.path.join("data", "stock_data.db")

@click.group()
def cli():
    """Simple CLI for viewing and refreshing stock data."""
    pass

@click.command()
@click.argument('ticker')
@click.option('--refresh/--no-refresh', default=False,
              help="If set, fetch & store the latest live data for the given ticker before display.")
def live(ticker, refresh):
    """
    Show the most recent live data for a given TICKER.

    Example usage:
      python cli.py live AAPL --refresh
    """
    logger.info(f"CLI live command called for ticker='{ticker}', refresh={refresh}")

    # 1) Optionally refresh the data via db_ingest
    if refresh:
        logger.info(f"Refreshing live data for {ticker} via db_ingest.")
        try:
            fetch_and_store_live_for_ticker(DB_FILE, ticker)
            logger.info(f"Successfully refreshed live data for {ticker}.")
        except Exception as e:
            logger.error(f"Live data refresh failed for {ticker}: {e}")
            click.echo(f"\n[ERROR] Could not refresh live data for {ticker}.\n")

    # 2) Query the database for the latest entry for this ticker
    logger.info(f"Fetching latest live data from DB for {ticker}")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Notice we reference 'LiveData' (capital L) to match your table, 
        # and we do a JOIN on Ticker to get the symbol
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

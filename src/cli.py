# cli.py

import sqlite3
import click
import os
from logs.logging import get_logger

# Import the new function to refresh just one ticker
from db_ingest import fetch_and_store_live_for_ticker

logger = get_logger()

# Adjust if your DB is stored elsewhere
DB_FILE = os.path.join("data", "stock_data_testing.db")

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
      live AAPL --refresh
    """
    click.secho(f"\n[INFO] Processing 'live' command for {ticker}", fg='yellow', bold=True)

    # 1) Optionally refresh the data
    if refresh:
        click.secho(f"Refreshing live data for {ticker} ...", fg='cyan')
        try:
            fetch_and_store_live_for_ticker(DB_FILE, ticker)
            click.secho(f"Successfully refreshed live data for {ticker}.", fg='green')
        except Exception as e:
            logger.error(f"Live data refresh failed for {ticker}: {e}")
            click.secho(f"\n[ERROR] Could not refresh live data for {ticker}.\n", fg='red')
            return

    # 2) Query the database for the latest entry for this ticker
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
            symbol, price, change, pct, ts = data

            click.secho("\n--- LIVE DATA ---", fg='blue', bold=True)
            click.echo(f" Ticker:         {symbol}")
            click.echo(f" Price:          ${price:.2f}")

            # Color-code the change
            if change is not None:
                color = 'green' if change >= 0 else 'red'
                click.secho(f" Change:         {change:.2f}", fg=color)
            else:
                click.echo(" Change:         N/A")

            # Color-code the percent change
            if pct is not None:
                color = 'green' if pct >= 0 else 'red'
                click.secho(f" Percent Change: {pct:.2f}%", fg=color)
            else:
                click.echo(" Percent Change: N/A")

            click.echo(f" Timestamp:      {ts}")
            click.secho("---\n", fg='blue', bold=True)

        else:
            click.secho(f"No live data found in the DB for ticker='{ticker}'.", fg='red')
    except Exception as e:
        logger.error(f"Error fetching live data for {ticker}: {e}")
        click.secho(f"[ERROR] Unable to retrieve live data for {ticker}", fg='red')


cli.add_command(live)

if __name__ == "__main__":
    cli()
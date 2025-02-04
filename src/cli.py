# cli.py

import sqlite3
import click
from logs.logging import get_logger

logger = get_logger()
DB_FILE = "data/stock_data.db"

@click.group()
def cli():
    pass

@click.command()
@click.argument('ticker')
def live(ticker):
    """Fetch live data for a given ticker."""
    logger.info(f"Fetching live data for {ticker}")
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM live_data WHERE ticker = ? ORDER BY timestamp DESC LIMIT 1", (ticker,))
        data = cursor.fetchone()
        if data:
            click.echo(f"\nTicker: {data[1]}")
            click.echo(f"Price: {data[2]}")
            click.echo(f"Change: {data[3]}")
            click.echo(f"Percent Change: {data[4]}")
            click.echo(f"Timestamp: {data[5]}\n")
        else:
            click.echo(f"No data found for {ticker}")
        conn.close()
    except Exception as e:
        logger.error(f"Error fetching live data for {ticker}: {e}")
        click.echo(f"Error fetching live data for {ticker}")

cli.add_command(live)

if __name__ == "__main__":
    cli()
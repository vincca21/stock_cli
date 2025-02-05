# scrape.py

"""
AUTHOR: Carter Vincent
DESCRIPTION: 
- Uses yahooquery to fetch stock data (live, daily, fundamentals, analysis)
- Saves two versions of the analysis data for each ticker:
  1) 'full_data' -> the complete, verbose data
  2) 'summary'   -> a simplified subset of key insights
- Avoids overwriting existing entries by merging all categories under each ticker.
- Saves final data to JSON (ready to be swapped out for SQLite DB later).
"""

import datetime
import json
import os
import time

import pandas as pd
from yahooquery import Ticker

from logs.logging import get_logger

# Configuration
ticker_list = ['AAPL', 'MSFT', 'GOOGL']  # Tickers for testing
DATA_DIR = "data"                        # Directory for storing data
os.makedirs(DATA_DIR, exist_ok=True)     # Create data directory if it doesn't exist
DATA_FILE = os.path.join(DATA_DIR, "stock_data.json")  # Path for storing data

logger = get_logger()

# -----------------------------
# Data Cleaning / Defaults
# -----------------------------

def safe_get(d, key, default=None):
    """
    Safe dictionary get with optional default.
    """
    val = d.get(key, default)
    logger.debug(f"safe_get: Looking for key='{key}', found='{val}', default='{default}'")
    return val

def clean_numeric(value, default=0.0):
    """
    Attempt to cast to float; return default on failure.
    """
    try:
        result = float(value)
        return result
    except (TypeError, ValueError) as e:
        logger.debug(f"clean_numeric: Unable to cast '{value}' to float, returning default='{default}' - {e}")
        return default

def clean_text(value, default=None):
    """
    Return stripped text if valid string, else return default.
    """
    if isinstance(value, str) and value.strip():
        return value.strip()
    logger.debug(f"clean_text: Using default='{default}' for value='{value}'")
    return default

def clean_list(value, default=None):
    """
    Return value if it's a list, else return default or empty list.
    """
    if isinstance(value, list):
        return value
    logger.debug(f"clean_list: '{value}' is not a list, returning default='{default}' or empty list")
    return default or []

# -----------------------------
# Helper to chunk tickers
# -----------------------------

def chunk_tickers(tickers, chunk_size=3):
    """
    Yield successive chunks of size `chunk_size` from the ticker list.
    """
    for i in range(0, len(tickers), chunk_size):
        yield tickers[i:i + chunk_size]

# -----------------------------
# Fetch Functions (Chunked)
# -----------------------------

def fetch_live_data(tickers):
    """
    Fetch live market data for tickers in chunks of 3.
    Returns a dict keyed by ticker.
    """
    logger.info(f"Fetching live data for {len(tickers)} tickers in chunks of 3.")
    results = {}
    # Break into chunks
    for chunk in chunk_tickers(tickers, chunk_size=3):
        logger.debug(f"Fetching live data chunk: {chunk}")
        # Create a Ticker object for this chunk
        ticker_obj = Ticker(chunk)
        price_data = ticker_obj.price

        for ticker in chunk:
            try:
                logger.debug(f"Fetching live data for ticker='{ticker}'.")
                market_data = price_data.get(ticker, {})
                # Build result with cleaning
                results[ticker] = {
                    'price': clean_numeric(safe_get(market_data, 'regularMarketPrice')),
                    'change': clean_numeric(safe_get(market_data, 'regularMarketChange')),
                    'percent_change': clean_numeric(safe_get(market_data, 'regularMarketChangePercent')),
                    'timestamp': datetime.datetime.now().isoformat()
                }
                logger.info(f"Live data fetch successful for {ticker}")
            except Exception as e:
                logger.error(f"Error fetching live data for {ticker}: {e}")
    return results

def fetch_daily_data(tickers):
    """
    Fetch daily-level data for tickers in chunks of 3.
    """
    logger.info(f"Fetching daily data for {len(tickers)} tickers in chunks of 3.")
    results = {}
    for chunk in chunk_tickers(tickers, chunk_size=3):
        logger.debug(f"Fetching daily data chunk: {chunk}")
        ticker_obj = Ticker(chunk)
        summary_detail_data = ticker_obj.summary_detail

        for ticker in chunk:
            try:
                logger.debug(f"Fetching daily data for ticker='{ticker}'.")
                summary_detail = summary_detail_data.get(ticker, {})
                results[ticker] = {
                    'open': clean_numeric(safe_get(summary_detail, 'open')),
                    'previous_close': clean_numeric(safe_get(summary_detail, 'previousClose')),
                    'day_high': clean_numeric(safe_get(summary_detail, 'dayHigh')),
                    'day_low': clean_numeric(safe_get(summary_detail, 'dayLow')),
                    'volume': int(clean_numeric(safe_get(summary_detail, 'volume'), default=0.0)),
                    'market_cap': int(clean_numeric(safe_get(summary_detail, 'marketCap'), default=0.0)),
                    'trailing_pe': clean_numeric(safe_get(summary_detail, 'trailingPE')),
                    'forward_pe': clean_numeric(safe_get(summary_detail, 'forwardPE')),
                    'timestamp': datetime.datetime.now().isoformat()
                }
                logger.info(f"Daily data fetch successful for {ticker}")
                logger.debug(f"Daily data details for {ticker}: {results[ticker]}")
            except Exception as e:
                logger.error(f"Error fetching daily data for {ticker}: {e}")
    return results

def fetch_fundamental_data(tickers):
    """
    Fetch rarely changing fundamental info for tickers in chunks of 3.
    """
    logger.info(f"Fetching fundamental data for {len(tickers)} tickers in chunks of 3.")
    results = {}
    for chunk in chunk_tickers(tickers, chunk_size=3):
        logger.debug(f"Fetching fundamental data chunk: {chunk}")
        ticker_obj = Ticker(chunk)
        asset_profile_data = ticker_obj.asset_profile

        for ticker in chunk:
            try:
                logger.debug(f"Fetching fundamental data for ticker='{ticker}'.")
                info = asset_profile_data.get(ticker, {})
                results[ticker] = {
                    'sector': clean_text(safe_get(info, 'sector')),
                    'industry': clean_text(safe_get(info, 'industry')),
                    'full_time_employees': int(clean_numeric(safe_get(info, 'fullTimeEmployees'), default=0.0)),
                    'country': clean_text(safe_get(info, 'country')),
                    'website': clean_text(safe_get(info, 'website')),
                    'description': clean_text(safe_get(info, 'longBusinessSummary')),
                    'timestamp': datetime.datetime.now().isoformat()
                }
                logger.info(f"Fundamental data fetch successful for {ticker}")
            except Exception as e:
                logger.error(f"Error fetching fundamental data for {ticker}: {e}")
    return results

def fetch_analysis_data(tickers):
    """
    Fetch multiple analysis-related endpoints in chunks of 3.
    Each chunk returns data for 'recommendation_trend', 'earnings_trend', 'index_trend'.
    We then construct:
      'analysis': {
         'full_data': { ... all raw info ... },
         'summary': { ... condensed version ... },
         'timestamp': ...
      }
    """
    logger.info(f"Fetching analysis data for {len(tickers)} tickers in chunks of 3.")
    results = {}

    for chunk in chunk_tickers(tickers, chunk_size=3):
        logger.debug(f"Fetching analysis data chunk: {chunk}")
        ticker_obj = Ticker(chunk)

        # Attempt to fetch each data set for this chunk
        try:
            rec_trend_df = ticker_obj.recommendation_trend
            logger.debug("Successfully fetched recommendation_trend.")
        except Exception as e:
            logger.error(f"Error fetching recommendation_trend for chunk {chunk}: {e}")
            rec_trend_df = pd.DataFrame()

        try:
            earnings_data = ticker_obj.earnings_trend
            logger.debug("Successfully fetched earnings_trend.")
        except Exception as e:
            logger.error(f"Error fetching earnings_trend for chunk {chunk}: {e}")
            earnings_data = {}

        try:
            index_data = ticker_obj.index_trend
            logger.debug(f"Index data: {index_data}")
        except Exception as e:
            logger.error(f"Error fetching index_trend for chunk {chunk}: {e}")
            index_data = {}

        def interpret_recommendation(counts):
            if not isinstance(counts, dict):
                return "Unknown"
            label_map = {
                'strongBuy': "Strong Buy",
                'buy': "Buy",
                'hold': "Hold",
                'sell': "Sell",
                'strongSell': "Strong Sell"
            }
            strong_buy = counts.get('strongBuy', 0)
            buy = counts.get('buy', 0)
            hold = counts.get('hold', 0)
            sell = counts.get('sell', 0)
            strong_sell = counts.get('strongSell', 0)
            mapping = {
                'strongBuy': strong_buy,
                'buy': buy,
                'hold': hold,
                'sell': sell,
                'strongSell': strong_sell
            }
            best_key = max(mapping, key=mapping.get)
            return label_map.get(best_key, "Unknown")

        def process_recommendation_trend(df, ticker_sym):
            """
            Return a dict with recommendation_trend data and a computed_recommendation.
            """
            if df.empty or ticker_sym not in df.index.levels[0]:
                logger.debug(f"No recommendation trend data found for ticker='{ticker_sym}'.")
                return {
                    'recommendation_trend': [],
                    'computed_recommendation': "No Data"
                }
            sub_df = df.xs(ticker_sym, level=0).copy()
            sub_df.reset_index(drop=True, inplace=True)
            rec_list = sub_df.to_dict(orient='records')

            # Usually '0m' is the current period
            row_0m = next((x for x in rec_list if x.get('period') == '0m'), None)
            if row_0m is None and len(rec_list) > 0:
                row_0m = rec_list[0]
            elif row_0m is None:
                row_0m = {}  # fallback

            recommendation = interpret_recommendation(row_0m)
            return {
                'recommendation_trend': rec_list,
                'computed_recommendation': recommendation
            }

        def create_summary(full_info):
            """
            Extract key fields from the full_info for a concise summary.
            """
            logger.debug("Creating summary from full_info for ticker.")
            rec_recommendation = full_info.get('computed_recommendation', "Unknown")
            idx_trend = full_info.get('index_trend', {})
            pe_ratio = idx_trend.get('peRatio', None)
            peg_ratio = idx_trend.get('pegRatio', None)
            ticker_earnings = full_info.get('earnings_trend', {})
            trend_list = ticker_earnings.get('trend', [])

            next_quarter = next((item for item in trend_list if item.get('period') == '+1q'), {})
            next_q_growth = next_quarter.get('growth')

            summary = {
                'recommendation': rec_recommendation,
                'pe_ratio': pe_ratio,
                'peg_ratio': peg_ratio,
                'next_quarter_growth': next_q_growth
            }
            logger.debug(f"Summary created: {summary}")
            return summary

        # Process for each ticker in this chunk
        for ticker in chunk:
            try:
                logger.debug(f"Fetching analysis data for ticker='{ticker}'.")
                rec_details = process_recommendation_trend(rec_trend_df, ticker)
                ticker_earnings = earnings_data.get(ticker, {})
                ticker_index = index_data.get(ticker, {})

                full_analysis = {
                    'recommendation_trend': rec_details.get('recommendation_trend', []),
                    'computed_recommendation': rec_details.get('computed_recommendation', "Unknown"),
                    'earnings_trend': ticker_earnings,
                    'index_trend': ticker_index
                }
                simplified = create_summary(full_analysis)

                results[ticker] = {
                    'analysis': {
                        'full_data': full_analysis,
                        'summary': simplified,
                        'timestamp': datetime.datetime.now().isoformat()
                    }
                }
                logger.info(f"Analysis data fetch successful for {ticker}")
            except Exception as e:
                logger.error(f"Error processing analysis data for {ticker}: {e}")
                results[ticker] = {
                    'analysis': {
                        'full_data': {},
                        'summary': {'recommendation': "Error"},
                        'timestamp': datetime.datetime.now().isoformat()
                    }
                }

    return results

# -----------------------------
# Data Storage / Combination
# -----------------------------

def combine_data_in_memory(live, daily, fundamentals, analysis):
    """
    Combine separate category data into a single structure keyed by ticker:
    combined_data[ticker] = {
        'live': {...},
        'daily': {...},
        'fundamentals': {...},
        'analysis': { 'full_data': {...}, 'summary': {...}, ...}
    }
    """
    logger.info("Combining data from all fetched categories.")
    combined = {}
    all_tickers = set(live.keys()) | set(daily.keys()) | set(fundamentals.keys()) | set(analysis.keys())
    logger.debug(f"Total tickers to combine: {len(all_tickers)}")

    for t in all_tickers:
        logger.debug(f"Combining data for ticker='{t}'")
        combined[t] = {
            'live': live.get(t, {}),
            'daily': daily.get(t, {}),
            'fundamentals': fundamentals.get(t, {})
        }
        if t in analysis:
            combined[t].update(analysis[t])

    logger.debug("Data combining complete.")
    return combined

def save_data(data, filename):
    """
    Save combined data to a JSON file, merging with any existing data.
    """
    logger.info(f"Saving data to '{filename}'.")
    try:
        if os.path.exists(filename):
            logger.debug("Existing data file found. Merging new data.")
            with open(filename, 'r') as f:
                existing_data = json.load(f)
        else:
            logger.debug("No existing data file found. Creating a new one.")
            existing_data = {}

        for ticker, record in data.items():
            if ticker not in existing_data:
                existing_data[ticker] = {}
            for category, cat_data in record.items():
                existing_data[ticker][category] = cat_data

        with open(filename, 'w') as f:
            json.dump(existing_data, f, indent=4)

        logger.info(f"Data saved successfully to {filename}")
    except Exception as e:
        logger.error(f"Error saving data to {filename}: {e}")

# -----------------------------
# Main Orchestration
# -----------------------------

def main():
    logger.info("Starting stock data collection...")
    start_time = time.time()

    # For demonstration, we fetch all categories
    step_start_time = time.time()
    live_data = fetch_live_data(ticker_list)
    logger.info(f"Fetched live data in {time.time() - step_start_time:.2f}s")

    step_start_time = time.time()
    daily_data = fetch_daily_data(ticker_list)
    logger.info(f"Fetched daily data in {time.time() - step_start_time:.2f}s")

    step_start_time = time.time()
    fundamental_data = fetch_fundamental_data(ticker_list)
    logger.info(f"Fetched fundamental data in {time.time() - step_start_time:.2f}s")

    step_start_time = time.time()
    analysis_data = fetch_analysis_data(ticker_list)  # returns both 'full_data' and 'summary'
    logger.info(f"Fetched analysis data in {time.time() - step_start_time:.2f}s")
    
    step_start_time = time.time()
    combined_data = combine_data_in_memory(live_data, daily_data, fundamental_data, analysis_data)
    logger.info(f"Combined data in {time.time() - step_start_time:.2f}s")

    step_start_time = time.time()
    save_data(combined_data, DATA_FILE)
    logger.info(f"Saved data in {time.time() - step_start_time:.2f}s")

    total_time = time.time() - start_time
    logger.info(f"Stock data collection completed in {total_time:.2f}s")
    logger.info(f"Data saved to {DATA_FILE}")
    logger.info("Exiting...")

if __name__ == "__main__":
    main()


"""
TODO:
- Add Functionality
    - Scheduling integration to run at specific times/intervals while the market is open
    - Single save function >> could increase performance
- Testing
    - Test the code with a larger list of tickers
- Documentation
- Cleanup
- Refactoring (if needed)
"""

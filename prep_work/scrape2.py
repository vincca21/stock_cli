# scrape2.py

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

import os
import datetime
import json
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
    return d.get(key, default)

def clean_numeric(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def clean_text(value, default=None):
    if isinstance(value, str) and value.strip():
        return value
    return default

def clean_list(value, default=None):
    if isinstance(value, list):
        return value
    return default or []

# -----------------------------
# Fetch Functions
# -----------------------------

def fetch_live_data(tickers):
    """
    Fetch live market data for tickers (batch call).
    Returns a dict keyed by ticker.
    """
    results = {}
    ticker_obj = Ticker(tickers)
    price_data = ticker_obj.price

    for ticker in tickers:
        try:
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
    Fetch daily-level data for tickers (batch call).
    """
    results = {}
    ticker_obj = Ticker(tickers)
    summary_detail_data = ticker_obj.summary_detail

    for ticker in tickers:
        try:
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
            # add print output with well-formatted json data for each ticker for debugging/testing
            print(json.dumps(results[ticker], indent=4))
        except Exception as e:
            logger.error(f"Error fetching daily data for {ticker}: {e}")
    return results

def fetch_fundamental_data(tickers):
    """
    Fetch rarely changing fundamental info for tickers (batch call).
    """
    results = {}
    ticker_obj = Ticker(tickers)
    asset_profile_data = ticker_obj.asset_profile

    for ticker in tickers:
        try:
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
    Fetch multiple analysis-related endpoints and store:
      'analysis': {
         'full_data': { ... all raw info ... },
         'summary': { ... condensed version ... },
         'timestamp': ...
      }
    """
    results = {}
    ticker_obj = Ticker(tickers)
    
    # 1) recommendation_trend → DataFrame
    try:
        rec_trend_df = ticker_obj.recommendation_trend
    except Exception as e:
        logger.error(f"Error fetching recommendation_trend: {e}")
        rec_trend_df = pd.DataFrame()
    
    # 2) earnings_trend → dict
    try:
        earnings_data = ticker_obj.earnings_trend
    except Exception as e:
        logger.error(f"Error fetching earnings_trend: {e}")
        earnings_data = {}
    
    # 3) index_trend → dict
    try:
        index_data = ticker_obj.index_trend
        logger.debug(f"Index data for debug: {index_data}")
    except Exception as e:
        logger.error(f"Error fetching index_trend: {e}")
        index_data = {}
    
    # Helper to interpret a row with strongBuy, buy, hold, sell, strongSell
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
        # for safety, get counts or default 0
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
        best_key = max(mapping, key=mapping.get)  # e.g. 'buy'
        return label_map.get(best_key, "Unknown")
    
    # Convert recommendation_trend DataFrame to dict per ticker
    def process_recommendation_trend(df, ticker):
        if df.empty or ticker not in df.index.levels[0]:
            return {
                'recommendation_trend': [],
                'computed_recommendation': "No Data"
            }
        sub_df = df.xs(ticker, level=0).copy()
        sub_df.reset_index(drop=True, inplace=True)
        
        rec_list = sub_df.to_dict(orient='records')
        row_0m = next((x for x in rec_list if x.get('period') == '0m'), None)
        if row_0m is None and len(rec_list) > 0:
            row_0m = rec_list[0]
        elif row_0m is None:
            row_0m = {}  # fallback if no rows
        recommendation = interpret_recommendation(row_0m)
        
        return {
            'recommendation_trend': rec_list,
            'computed_recommendation': recommendation
        }
    
    # Create a small summary from the full data
    def create_summary(full_info, ticker):
        """
        Extract just the most important fields or calculations from the full data
        """
        # This is arbitrary; define what's "key" for your scenario
        rec_recommendation = full_info.get('computed_recommendation', "Unknown")
        # Maybe we look at index_trend's peRatio, pegRatio
        index_trend = full_info.get('index_trend', {})
        pe_ratio = index_trend.get('peRatio', default=None)
        peg_ratio = index_trend.get('pegRatio', default=None)
        
        
        # Possibly check earnings trend for the next quarter growth
        ticker_earnings = full_info.get('earnings_trend', {})
        # We might store the entire dictionary or parse out "trend"
        trend_list = ticker_earnings.get('trend', [])
        next_quarter = next((item for item in trend_list if item.get('period') == '+1q'), {})
        next_q_growth = next_quarter.get('growth')  # just an example
        
        # Construct a short dictionary summarizing key points
        return {
            'recommendation': rec_recommendation,
            'pe_ratio': pe_ratio,
            'peg_ratio': peg_ratio,
            'next_quarter_growth': next_q_growth
        }
    
    # Build final results
    for ticker in tickers:
        try:
            rec_details = process_recommendation_trend(rec_trend_df, ticker)
            ticker_earnings = earnings_data.get(ticker, {})
            ticker_index = index_data.get(ticker, {})
            
            # We'll store everything under 'analysis'
            #   'full_data' = all the info
            #   'summary' = small subset
            # Then 'analysis_data[ticker]' is the top-level dict for this category
            full_analysis = {
                'recommendation_trend': rec_details.get('recommendation_trend', []),
                'computed_recommendation': rec_details.get('computed_recommendation', "Unknown"),
                'earnings_trend': ticker_earnings,
                'index_trend': ticker_index
            }
            
            simplified = create_summary(full_analysis, ticker)
            
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
            # Provide at least empty fallback so we don't break the entire dict
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
    combined = {}

    all_tickers = set(live.keys()) | set(daily.keys()) | set(fundamentals.keys()) | set(analysis.keys())

    for t in all_tickers:
        combined[t] = {
            'live': live.get(t, {}),
            'daily': daily.get(t, {}),
            'fundamentals': fundamentals.get(t, {})
        }
        
        # For analysis, we might have:
        #   analysis[t] = { 'analysis': {...} }
        # We can just merge that top-level dictionary
        # so the final structure has 'analysis' as a key
        # inside combined[t].
        if t in analysis:
            combined[t].update(analysis[t])

    return combined

def save_data(data, filename):
    """
    Save combined data to a JSON file, merging with any existing data.
    """
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                existing_data = json.load(f)
        else:
            existing_data = {}

        # Merge each ticker's data
        for ticker, record in data.items():
            if ticker not in existing_data:
                existing_data[ticker] = {}
            # Overwrite or merge categories
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
    # (Could implement selective fetching logic here)
    
    # 1. Fetch data categories
    step_start_time = time.time()
    live_data = fetch_live_data(ticker_list)
    # log completion w/ time 
    logger.info(f"Fetched live data in {time.time() - step_start_time:.2f} (s)")

    step_start_time = time.time()
    daily_data = fetch_daily_data(ticker_list)
    logger.info(f"Fetched daily data in {time.time() - step_start_time:.2f} (s)")

    step_start_time = time.time()
    fundamental_data = fetch_fundamental_data(ticker_list)
    logger.info(f"Fetched fundamental data in {time.time() - step_start_time:.2f} (s)")

    step_start_time = time.time()
    analysis_data = fetch_analysis_data(ticker_list)  # returns both 'full_data' and 'summary'
    logger.info(f"Fetched analysis data in {time.time() - step_start_time:.2f} (s)")
    
    # 2. Combine data into a single structure
    step_start_time = time.time()
    combined_data = combine_data_in_memory(live_data, daily_data, fundamental_data, analysis_data)
    logger.info(f"Combined data in {time.time() - step_start_time:.2f} (s)")

    # 3. Save combined data once
    step_start_time = time.time()
    save_data(combined_data, DATA_FILE)
    logger.info(f"Saved data in {time.time() - step_start_time:.2f} (s)")

    total_time = time.time() - start_time
    logger.info(f"Stock data collection completed in {total_time:.2f} (s)")
    logger.info(f"Data saved to {DATA_FILE}")
    logger.info("Exiting...")

if __name__ == "__main__":
    main()


"""
TODO:
- Add Functionality
    - Schedualing integration to run at specific times/intervals while the market is open
    - Single save function >> could increase performance
- Testing
    - Test the code with a larger list of tickers
- Documentation
- Cleanup
- Refactoring (if needed)
"""
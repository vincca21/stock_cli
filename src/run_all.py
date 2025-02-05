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
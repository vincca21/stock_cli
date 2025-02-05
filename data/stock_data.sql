-- stock_data.sql
-- Database for stock data & user data

CREATE TABLE Ticker (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT UNIQUE NOT NULL
);

CREATE TABLE LiveData (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER,
    price REAL,
    change REAL,
    percent_change REAL,
    timestamp TEXT,
    FOREIGN KEY (ticker_id) REFERENCES Ticker(id)
);

CREATE TABLE DailyData (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER,
    open REAL,
    previous_close REAL,
    day_high REAL,
    day_low REAL,
    volume INTEGER,
    market_cap INTEGER,
    trailing_pe REAL,
    forward_pe REAL,
    timestamp TEXT,
    FOREIGN KEY (ticker_id) REFERENCES Ticker(id)
);

CREATE TABLE Fundamentals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER,
    sector TEXT,
    industry TEXT,
    full_time_employees INTEGER,
    country TEXT,
    website TEXT,
    description TEXT,
    timestamp TEXT,
    FOREIGN KEY (ticker_id) REFERENCES Ticker(id)
);

CREATE TABLE Analysis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER,
    computed_recommendation TEXT,
    pe_ratio REAL,
    peg_ratio REAL,
    next_quarter_growth REAL,
    timestamp TEXT,
    FOREIGN KEY (ticker_id) REFERENCES Ticker(id)
);

CREATE TABLE RecommendationTrend (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id INTEGER,
    period TEXT,
    strongBuy INTEGER,
    buy INTEGER,
    hold INTEGER,
    sell INTEGER,
    strongSell INTEGER,
    FOREIGN KEY (analysis_id) REFERENCES Analysis(id)
);

CREATE TABLE EarningsTrend (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id INTEGER,
    period TEXT,
    growth REAL,
    FOREIGN KEY (analysis_id) REFERENCES Analysis(id)
);

CREATE TABLE IndexTrend (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    analysis_id INTEGER,
    peRatio REAL,
    pegRatio REAL,
    FOREIGN KEY (analysis_id) REFERENCES Analysis(id)
);
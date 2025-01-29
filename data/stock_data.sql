-- stock_data.sql
-- Database for stock data & user data

-- Table for live (most recent) stock data
CREATE TABLE stock_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10, 2),
    high DECIMAL(10, 2),
    low DECIMAL(10, 2),
    close DECIMAL(10, 2),
    volume INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);

-- Table for historical stock data organized by date for each ticker
CREATE TABLE historical_stock_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(10, 2),
    high DECIMAL(10, 2),
    low DECIMAL(10, 2),
    close DECIMAL(10, 2),
    volume INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);


-- Table for user data
CREATE TABLE user_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    user_name VARCHAR(50) NOT NULL,
);
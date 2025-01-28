-- stock_data.sql
-- Database for stock data & user data

-- Table for live (most recent) stock data
CREATE TABLE stock_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker VARCHAR(10),
    price FLOAT (10, 2) NOT NULL,
    change FLOAT (10, 2) NOT NULL,
    percent_change FLOAT (10, 2) NOT NULL,
    time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    foreign key (ticker) references stock_data_general(ticker)
    foreign key (ticker) references stock_data_prev(ticker)
);

-- Table for previous most recent stock data
CREATE TABLE stock_data_prev (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker VARCHAR(10),
    price FLOAT (10, 2) NOT NULL,
    change FLOAT (10, 2) NOT NULL,
    percent_change FLOAT (10, 2) NOT NULL,
    time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    foreign key (ticker) references stock_data(ticker)
    foreign key (ticker) references stock_data_general(ticker)
);

-- Table for general stock data
CREATE TABLE stock_data_general (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker VARCHAR(10),
    open FLOAT (10, 2) NOT NULL,
    high FLOAT (10, 2) NOT NULL,
    low FLOAT (10, 2) NOT NULL,
    volume INTEGER NOT NULL,
    time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    foreign key (ticker) references stock_data(ticker)
    foreign key (ticker) references stock_data_prev(ticker)
);

-- Table for user data
CREATE TABLE user_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    user_name VARCHAR(50) NOT NULL,
);
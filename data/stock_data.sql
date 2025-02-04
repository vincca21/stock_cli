-- stock_data.sql
-- Database for stock data & user data

-- Table for each stock, id, name, and references to differnt tables of info of varying frequency
CREATE TABLE stock (
    id INT PRIMARY KEY AUTO_INCREMENT,
    ticker VARCHAR(10) NOT NULL,
    name VARCHAR(100) NOT NULL,
    live_data INT,
    frequent_data INT,
    infrequent_data INT,
    general_data INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    foreign key (live_data) references live_data(id),
    foreign key (frequent_data) references frequent_data(id),
    foreign key (infrequent_data) references infrequent_data(id),
    foreign key (general_data) references general_data(id)
);

-- Table for live data of stock (5 second intervals) - most relevent data for changing info
CREATE TABLE live_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_id INT,
    price DECIMAL(10, 2),
    change DECIMAL(10, 2),
    percent_change DECIMAL(10, 2),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    foreign key (stock_id) references stock(id)
);

-- Table for frequent data of stock (1 minute intervals) - less relevent data for changing info
CREATE TABLE frequent_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_id INT,
    previous_close DECIMAL(10, 2),
    open DECIMAL(10, 2),
    day_high DECIMAL(10, 2),
    day_low DECIMAL(10, 2),
    volume BIGINT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    foreign key (stock_id) references stock(id)
);

-- Table for infrequent data of stock (1 day intervals) - least relevent data for changing info
CREATE TABLE infrequent_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    stock_id INT,
    market_cap BIGINT,
    pe_ratio DECIMAL(10, 2),
    dividend_yield DECIMAL(10, 2),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    foreign key (stock_id) references stock(id)
);
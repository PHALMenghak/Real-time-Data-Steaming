-- =====================================================
-- Create Raw Stock Data Table
-- =====================================================

CREATE TABLE IF NOT EXISTS raw_stock_data (

    id BIGSERIAL PRIMARY KEY,

    symbol VARCHAR(10) NOT NULL,

    price DOUBLE PRECISION NOT NULL,

    volume BIGINT NOT NULL,

    event_time TIMESTAMP NOT NULL,

    ingestion_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

-- =====================================================
-- Create Processed Stock Data Table
-- =====================================================

CREATE TABLE IF NOT EXISTS processed_stock_data (

    id BIGSERIAL PRIMARY KEY,

    symbol VARCHAR(10) NOT NULL,

    window_start TIMESTAMP NOT NULL,

    window_end TIMESTAMP NOT NULL,

    moving_avg_price DOUBLE PRECISION,

    price_volatility DOUBLE PRECISION,

    total_volume BIGINT,

    max_price DOUBLE PRECISION,

    min_price DOUBLE PRECISION,

    price_change_pct DOUBLE PRECISION,

    volume_spike BOOLEAN,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

-- =====================================================
-- Raw Table Indexes
-- =====================================================

CREATE INDEX idx_raw_symbol
ON raw_stock_data(symbol);

CREATE INDEX idx_raw_event_time
ON raw_stock_data(event_time);

CREATE INDEX idx_raw_symbol_time
ON raw_stock_data(symbol, event_time);

-- =====================================================
-- Processed Table Indexes
-- =====================================================

CREATE INDEX idx_processed_symbol
ON processed_stock_data(symbol);

CREATE INDEX idx_processed_window_start
ON processed_stock_data(window_start);

CREATE INDEX idx_processed_symbol_window
ON processed_stock_data(symbol, window_start);
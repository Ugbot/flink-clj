-- Initialize PostgreSQL for flink-clj testing
-- This script runs automatically when the container starts

-- Create a table for testing Flink JDBC sink
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    user_id VARCHAR(100),
    payload JSONB,
    event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a table for word count results
CREATE TABLE IF NOT EXISTS word_counts (
    word VARCHAR(255) PRIMARY KEY,
    count BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a table for aggregation results
CREATE TABLE IF NOT EXISTS aggregations (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    key VARCHAR(100) NOT NULL,
    sum_value DOUBLE PRECISION,
    count_value BIGINT,
    avg_value DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end, key)
);

-- Create a source table for CDC testing
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    order_status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert some sample data
INSERT INTO orders (customer_id, product_name, quantity, price, order_status) VALUES
    ('customer-001', 'Widget A', 5, 19.99, 'completed'),
    ('customer-002', 'Widget B', 3, 29.99, 'pending'),
    ('customer-001', 'Gadget X', 1, 99.99, 'shipped'),
    ('customer-003', 'Widget A', 10, 19.99, 'completed');

-- Create index for common queries
CREATE INDEX IF NOT EXISTS idx_events_event_time ON events(event_time);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(order_status);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO flink;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO flink;

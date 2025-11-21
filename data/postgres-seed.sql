-- PostgreSQL seed script for ETL testing
-- Run this to set up sample data in a PostgreSQL database

-- Create sales table
CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    amount DECIMAL(10, 2) NOT NULL CHECK (amount > 0),
    sale_date TIMESTAMP NOT NULL,
    region VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price > 0),
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    segment VARCHAR(50) NOT NULL,
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO sales (id, product_id, customer_id, quantity, amount, sale_date, region) VALUES
(1, 'P001', 'C001', 5, 150.00, '2024-01-15 10:30:00', 'North'),
(2, 'P002', 'C002', 3, 225.00, '2024-01-16 14:20:00', 'South'),
(3, 'P001', 'C003', 2, 60.00, '2024-01-17 09:15:00', 'East'),
(4, 'P003', 'C001', 1, 499.99, '2024-01-18 16:45:00', 'North'),
(5, 'P002', 'C004', 4, 300.00, '2024-01-19 11:30:00', 'West'),
(6, 'P003', 'C002', 2, 999.98, '2024-01-20 13:00:00', 'South'),
(7, 'P001', 'C005', 10, 300.00, '2024-01-21 10:00:00', 'East'),
(8, 'P002', 'C003', 1, 75.00, '2024-01-22 15:30:00', 'North'),
(9, 'P003', 'C004', 1, 499.99, '2024-01-23 12:00:00', 'West'),
(10, 'P001', 'C005', 3, 90.00, '2024-01-24 14:15:00', 'East')
ON CONFLICT (id) DO NOTHING;

INSERT INTO products (product_id, product_name, category, price, valid_from) VALUES
('P001', 'Widget A', 'Electronics', 30.00, '2024-01-01 00:00:00'),
('P002', 'Gadget B', 'Electronics', 75.00, '2024-01-01 00:00:00'),
('P003', 'Device C', 'Electronics', 499.99, '2024-01-01 00:00:00')
ON CONFLICT (product_id) DO NOTHING;

INSERT INTO customers (customer_id, customer_name, email, segment, valid_from) VALUES
('C001', 'John Doe', 'john@example.com', 'Premium', '2024-01-01 00:00:00'),
('C002', 'Jane Smith', 'jane@example.com', 'Standard', '2024-01-01 00:00:00'),
('C003', 'Bob Johnson', 'bob@example.com', 'Premium', '2024-01-01 00:00:00'),
('C004', 'Alice Brown', 'alice@example.com', 'Standard', '2024-01-01 00:00:00'),
('C005', 'Charlie Wilson', 'charlie@example.com', 'Premium', '2024-01-01 00:00:00')
ON CONFLICT (customer_id) DO NOTHING;

-- Create update trigger to automatically update updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_sales_updated_at BEFORE UPDATE ON sales
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_products_updated_at BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_customers_updated_at BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

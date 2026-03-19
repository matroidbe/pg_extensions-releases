-- =============================================================================
-- pg_feature Demo Dataset
-- =============================================================================
-- This schema is inspired by the Featuretools retail demo dataset.
-- It demonstrates a typical e-commerce data model with:
--   - customers (target entity for feature generation)
--   - orders (child of customers)
--   - order_items (child of orders, grandchild of customers)
--   - products (reference table)
--
-- This allows demonstrating:
--   - Depth-1 features: aggregations from orders to customers
--   - Depth-2 features: aggregations from order_items through orders to customers
--   - Time-based features: cutoff times for ML training
--   - Transform features: extracting year/month from timestamps
-- =============================================================================

-- Drop existing tables if they exist
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- =============================================================================
-- Customers Table (Target Entity)
-- =============================================================================
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    country VARCHAR(50) NOT NULL,
    signup_date DATE NOT NULL,
    is_premium BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- Products Table (Reference/Dimension)
-- =============================================================================
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- Orders Table (Child of Customers)
-- =============================================================================
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    shipping_country VARCHAR(50),
    total_amount DECIMAL(10,2),
    is_cancelled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);

-- =============================================================================
-- Order Items Table (Child of Orders, Grandchild of Customers)
-- =============================================================================
CREATE TABLE order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL DEFAULT 1,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);

-- =============================================================================
-- Sample Products (20 products across 4 categories)
-- =============================================================================
INSERT INTO products (product_name, category, unit_price) VALUES
    -- Electronics
    ('Wireless Mouse', 'Electronics', 29.99),
    ('USB-C Hub', 'Electronics', 49.99),
    ('Bluetooth Headphones', 'Electronics', 79.99),
    ('Webcam HD', 'Electronics', 59.99),
    ('Portable Charger', 'Electronics', 24.99),
    -- Clothing
    ('Cotton T-Shirt', 'Clothing', 19.99),
    ('Denim Jeans', 'Clothing', 49.99),
    ('Winter Jacket', 'Clothing', 89.99),
    ('Running Shoes', 'Clothing', 69.99),
    ('Wool Sweater', 'Clothing', 54.99),
    -- Home & Garden
    ('Coffee Maker', 'Home', 39.99),
    ('Desk Lamp', 'Home', 34.99),
    ('Plant Pot Set', 'Home', 22.99),
    ('Kitchen Scale', 'Home', 19.99),
    ('Throw Blanket', 'Home', 29.99),
    -- Books
    ('Python Programming', 'Books', 44.99),
    ('Data Science Handbook', 'Books', 39.99),
    ('Machine Learning Guide', 'Books', 54.99),
    ('SQL Fundamentals', 'Books', 34.99),
    ('Statistics for ML', 'Books', 49.99);

-- =============================================================================
-- Sample Customers (50 customers from different countries)
-- =============================================================================
INSERT INTO customers (customer_name, email, country, signup_date, is_premium) VALUES
    ('Alice Johnson', 'alice@example.com', 'USA', '2023-01-15', TRUE),
    ('Bob Smith', 'bob@example.com', 'USA', '2023-02-20', FALSE),
    ('Carol Williams', 'carol@example.com', 'UK', '2023-01-10', TRUE),
    ('David Brown', 'david@example.com', 'Canada', '2023-03-05', FALSE),
    ('Emma Davis', 'emma@example.com', 'Australia', '2023-02-28', TRUE),
    ('Frank Miller', 'frank@example.com', 'USA', '2023-04-12', FALSE),
    ('Grace Wilson', 'grace@example.com', 'UK', '2023-03-18', TRUE),
    ('Henry Taylor', 'henry@example.com', 'Germany', '2023-05-01', FALSE),
    ('Ivy Anderson', 'ivy@example.com', 'France', '2023-04-22', TRUE),
    ('Jack Thomas', 'jack@example.com', 'USA', '2023-06-08', FALSE),
    ('Kate Martinez', 'kate@example.com', 'Spain', '2023-05-15', TRUE),
    ('Leo Garcia', 'leo@example.com', 'Mexico', '2023-06-20', FALSE),
    ('Mia Robinson', 'mia@example.com', 'Canada', '2023-07-04', TRUE),
    ('Noah Clark', 'noah@example.com', 'USA', '2023-07-19', FALSE),
    ('Olivia Lewis', 'olivia@example.com', 'UK', '2023-08-02', TRUE),
    ('Paul Walker', 'paul@example.com', 'Australia', '2023-08-15', FALSE),
    ('Quinn Hall', 'quinn@example.com', 'USA', '2023-09-01', TRUE),
    ('Ruby Allen', 'ruby@example.com', 'Germany', '2023-09-12', FALSE),
    ('Sam Young', 'sam@example.com', 'France', '2023-10-05', TRUE),
    ('Tina King', 'tina@example.com', 'Italy', '2023-10-18', FALSE),
    ('Uma Wright', 'uma@example.com', 'USA', '2023-11-02', TRUE),
    ('Victor Scott', 'victor@example.com', 'UK', '2023-11-15', FALSE),
    ('Wendy Green', 'wendy@example.com', 'Canada', '2023-12-01', TRUE),
    ('Xavier Adams', 'xavier@example.com', 'USA', '2023-12-12', FALSE),
    ('Yara Nelson', 'yara@example.com', 'Spain', '2024-01-05', TRUE),
    ('Zack Hill', 'zack@example.com', 'Mexico', '2024-01-18', FALSE),
    ('Amy Baker', 'amy@example.com', 'USA', '2024-02-02', TRUE),
    ('Ben Carter', 'ben@example.com', 'UK', '2024-02-15', FALSE),
    ('Chloe Mitchell', 'chloe@example.com', 'Australia', '2024-03-01', TRUE),
    ('Dan Perez', 'dan@example.com', 'USA', '2024-03-12', FALSE),
    ('Eva Roberts', 'eva@example.com', 'Germany', '2024-04-05', TRUE),
    ('Finn Turner', 'finn@example.com', 'France', '2024-04-18', FALSE),
    ('Gina Phillips', 'gina@example.com', 'Italy', '2024-05-02', TRUE),
    ('Hugo Campbell', 'hugo@example.com', 'USA', '2024-05-15', FALSE),
    ('Iris Parker', 'iris@example.com', 'UK', '2024-06-01', TRUE),
    ('Jake Evans', 'jake@example.com', 'Canada', '2024-06-12', FALSE),
    ('Kim Edwards', 'kim@example.com', 'USA', '2024-07-05', TRUE),
    ('Luke Collins', 'luke@example.com', 'Australia', '2024-07-18', FALSE),
    ('Maya Stewart', 'maya@example.com', 'Germany', '2024-08-02', TRUE),
    ('Nate Sanchez', 'nate@example.com', 'Spain', '2024-08-15', FALSE),
    ('Olive Morris', 'olive@example.com', 'USA', '2024-09-01', TRUE),
    ('Pete Rogers', 'pete@example.com', 'Mexico', '2024-09-12', FALSE),
    ('Quincy Reed', 'quincy@example.com', 'UK', '2024-10-05', TRUE),
    ('Rose Cook', 'rose@example.com', 'France', '2024-10-18', FALSE),
    ('Steve Morgan', 'steve@example.com', 'USA', '2024-11-02', TRUE),
    ('Tara Bell', 'tara@example.com', 'Italy', '2024-11-15', FALSE),
    ('Ulysses Murphy', 'ulysses@example.com', 'Canada', '2024-12-01', TRUE),
    ('Vera Bailey', 'vera@example.com', 'USA', '2024-12-12', FALSE),
    ('Will Rivera', 'will@example.com', 'UK', '2024-12-20', TRUE),
    ('Xena Cooper', 'xena@example.com', 'Australia', '2024-12-28', FALSE);

-- =============================================================================
-- Generate Orders (varying patterns per customer)
-- =============================================================================
-- This creates realistic order patterns:
--   - Premium customers tend to order more frequently
--   - Orders span from 2023 to 2024
--   - Mix of completed, shipped, and cancelled orders

DO $$
DECLARE
    cust RECORD;
    num_orders INTEGER;
    order_date TIMESTAMPTZ;
    order_status VARCHAR(20);
    is_cancel BOOLEAN;
    i INTEGER;
BEGIN
    FOR cust IN SELECT customer_id, signup_date, is_premium FROM customers LOOP
        -- Premium customers: 5-15 orders, Regular: 1-8 orders
        IF cust.is_premium THEN
            num_orders := 5 + floor(random() * 11)::int;
        ELSE
            num_orders := 1 + floor(random() * 8)::int;
        END IF;

        FOR i IN 1..num_orders LOOP
            -- Random date between signup and end of 2024
            order_date := cust.signup_date + (random() * (DATE '2024-12-31' - cust.signup_date))::int * INTERVAL '1 day'
                         + (random() * 24)::int * INTERVAL '1 hour';

            -- Random status with realistic distribution
            CASE floor(random() * 10)::int
                WHEN 0 THEN order_status := 'pending'; is_cancel := FALSE;
                WHEN 1 THEN order_status := 'cancelled'; is_cancel := TRUE;
                WHEN 2, 3 THEN order_status := 'shipped'; is_cancel := FALSE;
                ELSE order_status := 'completed'; is_cancel := FALSE;
            END CASE;

            INSERT INTO orders (customer_id, order_date, status, shipping_country, is_cancelled)
            SELECT cust.customer_id, order_date, order_status,
                   (SELECT country FROM customers WHERE customer_id = cust.customer_id),
                   is_cancel;
        END LOOP;
    END LOOP;
END $$;

-- =============================================================================
-- Generate Order Items (1-5 items per order)
-- =============================================================================
DO $$
DECLARE
    ord RECORD;
    num_items INTEGER;
    prod_id INTEGER;
    qty INTEGER;
    price DECIMAL(10,2);
    discount DECIMAL(5,2);
    i INTEGER;
BEGIN
    FOR ord IN SELECT order_id FROM orders LOOP
        -- 1-5 items per order
        num_items := 1 + floor(random() * 5)::int;

        FOR i IN 1..num_items LOOP
            -- Random product
            SELECT product_id, unit_price INTO prod_id, price
            FROM products ORDER BY random() LIMIT 1;

            -- Random quantity 1-3
            qty := 1 + floor(random() * 3)::int;

            -- Random discount 0-20%
            discount := floor(random() * 21)::decimal;

            INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_percent)
            VALUES (ord.order_id, prod_id, qty, price, discount);
        END LOOP;
    END LOOP;
END $$;

-- =============================================================================
-- Update order totals
-- =============================================================================
UPDATE orders o
SET total_amount = (
    SELECT COALESCE(SUM(quantity * unit_price * (1 - discount_percent/100)), 0)
    FROM order_items oi
    WHERE oi.order_id = o.order_id
);

-- =============================================================================
-- Create Training Labels Table (for ML demonstration)
-- =============================================================================
-- This table defines cutoff times and labels for churn prediction
-- The label indicates if the customer made a purchase in the 30 days AFTER the cutoff

DROP TABLE IF EXISTS churn_labels CASCADE;

CREATE TABLE churn_labels (
    customer_id INTEGER PRIMARY KEY REFERENCES customers(customer_id),
    cutoff_time TIMESTAMPTZ NOT NULL,
    churned BOOLEAN NOT NULL
);

-- Generate labels: cutoff at 2024-10-01, churn if no orders in next 30 days
INSERT INTO churn_labels (customer_id, cutoff_time, churned)
SELECT
    c.customer_id,
    '2024-10-01 00:00:00'::timestamptz AS cutoff_time,
    NOT EXISTS (
        SELECT 1 FROM orders o
        WHERE o.customer_id = c.customer_id
        AND o.order_date > '2024-10-01'
        AND o.order_date <= '2024-10-31'
        AND NOT o.is_cancelled
    ) AS churned
FROM customers c
WHERE c.signup_date < '2024-10-01';

-- =============================================================================
-- Summary Statistics
-- =============================================================================
SELECT 'customers' AS table_name, COUNT(*) AS row_count FROM customers
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL
SELECT 'churn_labels', COUNT(*) FROM churn_labels;

-- =============================================================================
-- Sample Queries to Verify Data
-- =============================================================================

-- Customer order summary
SELECT
    c.customer_id,
    c.customer_name,
    c.is_premium,
    COUNT(DISTINCT o.order_id) AS order_count,
    COALESCE(SUM(o.total_amount), 0) AS total_spent,
    MIN(o.order_date) AS first_order,
    MAX(o.order_date) AS last_order
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id AND NOT o.is_cancelled
GROUP BY c.customer_id, c.customer_name, c.is_premium
ORDER BY total_spent DESC
LIMIT 10;

-- =============================================================================
-- pg_feature Usage Examples
-- =============================================================================
-- After loading the extension, run these commands:

-- 1. Load the extension
-- CREATE EXTENSION pg_feature;

-- 2. Auto-discover relationships from foreign keys
-- SELECT pgft.discover_relationships('public');

-- 3. Set time indexes for temporal filtering
-- SELECT pgft.set_time_index('public.orders', 'order_date');
-- SELECT pgft.set_time_index('public.order_items', 'created_at');

-- 4. Generate features for customers (depth 2 = includes order_items aggregations)
-- SELECT * FROM pgft.synthesize_features(
--     project_name := 'customer_360',
--     target_table := 'public.customers',
--     target_id := 'customer_id',
--     max_depth := 2,
--     output_name := 'public.customer_features'
-- );

-- 5. Generate features with cutoff time (for ML training)
-- SELECT * FROM pgft.synthesize_features(
--     project_name := 'churn_model_v1',
--     target_table := 'public.customers',
--     target_id := 'customer_id',
--     cutoff_times_table := 'public.churn_labels',
--     cutoff_time_column := 'cutoff_time',
--     max_depth := 2,
--     output_name := 'public.churn_features'
-- );

-- 6. List generated features
-- SELECT * FROM pgft.list_features('customer_360');

-- 7. Explain a specific feature
-- SELECT pgft.explain_feature('customer_360', 'orders_COUNT');

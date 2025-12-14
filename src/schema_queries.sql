-- 1. Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    f_name TEXT,
    l_name TEXT,
    segment TEXT,
    city TEXT,
    state TEXT,
    country TEXT
);

-- 2. Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_card_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category_name TEXT,
    department_name TEXT,
    product_price REAL
);

-- 3. Dimension: Location
CREATE TABLE IF NOT EXISTS dim_location (
    location_id INTEGER PRIMARY KEY,
    market TEXT,
    order_region TEXT,
    order_country TEXT,
    order_city TEXT
);

-- 4. Fact Table: Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id INTEGER,
    order_item_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    product_card_id INTEGER,
    location_id INTEGER,
    order_date DATETIME,
    shipping_date DATETIME,
    shipping_mode TEXT,
    days_scheduled INTEGER,
    days_real INTEGER,
    delivery_status TEXT,
    order_status TEXT,  -- Added this column
    benefit_per_order REAL,
    sales_amount REAL,
    order_quantity INTEGER,
    late_delivery_risk INTEGER,
    FOREIGN KEY(customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY(product_card_id) REFERENCES dim_products(product_card_id),
    FOREIGN KEY(location_id) REFERENCES dim_location(location_id)
);
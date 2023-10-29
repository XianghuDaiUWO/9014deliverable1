-- CREATE DATABASE mydatabase;

DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

CREATE TABLE geolocation_table(
    geolocation_zip_code_prefix CHAR(5) CHECK (geolocation_zip_code_prefix ~ '^[0-9]{5}$'),
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR(60) NOT NULL,
    geolocation_state VARCHAR(5) NOT NULL,
    CONSTRAINT pk_geolocation_table PRIMARY KEY (geolocation_zip_code_prefix)
);

CREATE TABLE customer_table (
    customer_unique_id VARCHAR(64),
    customer_id VARCHAR(64),
    customer_zip_code_prefix CHAR(5) CHECK (customer_zip_code_prefix ~ '^[0-9]{5}$'),
    CONSTRAINT pk_customer_table PRIMARY KEY (customer_id),
    CONSTRAINT fk_customer_table FOREIGN KEY (customer_zip_code_prefix) REFERENCES geolocation_table(geolocation_zip_code_prefix)
);

CREATE TABLE seller_table(
    seller_id VARCHAR(64),
    seller_zip_code_prefix CHAR(5) CHECK (seller_zip_code_prefix ~ '^[0-9]{5}$'),
    CONSTRAINT pk_seller_table PRIMARY KEY (seller_id),
    CONSTRAINT fk_seller_table FOREIGN KEY (seller_zip_code_prefix) REFERENCES geolocation_table(geolocation_zip_code_prefix)
);

CREATE TABLE order_table (
    order_id VARCHAR(64),
    customer_id VARCHAR(64),
    order_status VARCHAR(12) CHECK (
        order_status IN ('approved', 'canceled', 'created', 'delivered', 'invoiced', 'processing', 'shipped', 'unavailable')
    ),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    CONSTRAINT pk_order_table PRIMARY KEY (order_id),
    CONSTRAINT fk_order_table FOREIGN KEY (customer_id) REFERENCES customer_table(customer_id)
);

CREATE TABLE review_table (
    review_id VARCHAR(64),
    order_id VARCHAR(64) NOT NULL,
    review_score INT CHECK (review_score BETWEEN 1 AND 5), 
    review_comment_title VARCHAR(255), 
    review_comment_message TEXT, 
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    PRIMARY KEY (review_id, order_id),
    CONSTRAINT fk_review_table_order_id FOREIGN KEY (order_id) REFERENCES order_table(order_id)
);

CREATE TABLE payment_table (
    order_id VARCHAR(64) NOT NULL,
    payment_sequencial INT NOT NULL,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value FLOAT,
    PRIMARY KEY (order_id, payment_sequencial),
    CONSTRAINT fk_payment_table_order_id FOREIGN KEY (order_id) REFERENCES order_table(order_id)
);

CREATE TABLE product_table (
    product_id VARCHAR(64),
    product_category_name VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
    CONSTRAINT pk_product_table PRIMARY KEY (product_id)
);

CREATE TABLE Category_Translation (
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    CONSTRAINT pk_category_translation PRIMARY KEY (product_category_name)
);

CREATE TABLE Order_Item (
    order_id VARCHAR(64) NOT NULL,           
    product_id VARCHAR(64) NOT NULL,           
    order_item_id INT NOT NULL,            
    seller_id VARCHAR(64) NOT NULL,                
    shipping_limit_date TIMESTAMP NOT NULL,
    price DECIMAL(10,2) NOT NULL,          
    freight_value DECIMAL(10,2) NOT NULL,  

    CONSTRAINT pk_order_item PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT fk_orderitem_order FOREIGN KEY (order_id) REFERENCES order_table(order_id),  -- Modifying Table & Column Names
    CONSTRAINT fk_orderitem_order2 FOREIGN KEY (product_id) REFERENCES product_table(product_id),
    CONSTRAINT fk_orderitem_order3 FOREIGN KEY (seller_id) REFERENCES seller_table(seller_id)
);

-- Populate values into geolocation_table

-- Firstly create a temp table and copy all values from source csv file to the temp table
CREATE TEMPORARY TABLE temp_geolocation_table(
    column1 CHAR(5),
    column2 FLOAT NOT NULL,
    column3 FLOAT NOT NULL,
    column4 VARCHAR(60),
    column5 VARCHAR(5)
);

\COPY temp_geolocation_table FROM '/Users/xavierd/Downloads/dataset/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;

-- Delete repeat values inside the temp table

DELETE FROM temp_geolocation_table
WHERE ctid IN (
    SELECT ctid
    FROM (
        SELECT ctid,
        ROW_NUMBER() OVER (PARTITION BY column1 ORDER BY ctid DESC) as row_num
        FROM temp_geolocation_table
    ) t
    WHERE t.row_num > 1
);

-- populate geolocation_table with the value in temp table
INSERT INTO geolocation_table (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
SELECT column1, column2, column3, column4, column5 FROM temp_geolocation_table;

-- drop temp table
DROP TABLE temp_geolocation_table;


-- populate values into customer_table
CREATE TEMPORARY TABLE temp_customer_table(
    column1 VARCHAR(64),
    column2 VARCHAR(64),
    column3 CHAR(5),
    column4 VARCHAR(80),
    column5 VARCHAR(80)
);

\COPY temp_customer_table FROM '/Users/xavierd/Downloads/dataset/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;

-- some customers' zip code prefix is not recorded in the geolocation_table, so we need to add them into the geolocation_table first before inserting values into the customer table
INSERT INTO geolocation_table (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
SELECT DISTINCT
    t.column3, 
    CAST(NULL AS double precision) as geolocation_lat, 
    CAST(NULL AS double precision) as geolocation_lng, 
    t.column4, 
    t.column5
FROM 
    temp_customer_table t
WHERE NOT EXISTS (
    SELECT 1
    FROM geolocation_table g
    WHERE t.column3 = g.geolocation_zip_code_prefix
);

INSERT INTO customer_table (customer_id, customer_unique_id, customer_zip_code_prefix)
SELECT column1, column2, column3 FROM temp_customer_table;

DROP TABLE temp_customer_table;


-- pupulate values into the seller_table
CREATE TEMPORARY TABLE temp_seller_table(
    column1 VARCHAR(64),
    column2 CHAR(5),
    column3 VARCHAR(80),
    column4 VARCHAR(5)
);

\COPY temp_seller_table FROM '/Users/xavierd/Downloads/dataset/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;

INSERT INTO geolocation_table (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
SELECT DISTINCT
    t.column2, 
    CAST(NULL AS double precision) as geolocation_lat, 
    CAST(NULL AS double precision) as geolocation_lng, 
    t.column3,
    t.column4
FROM 
    temp_seller_table t
WHERE NOT EXISTS (
    SELECT 1
    FROM geolocation_table g
    WHERE t.column2 = g.geolocation_zip_code_prefix
);

INSERT INTO seller_table (seller_id, seller_zip_code_prefix)
SELECT column1, column2 FROM temp_seller_table;

DROP TABLE temp_seller_table;

-- populate values into the order_table
CREATE TEMPORARY TABLE temp_order_table (
    column1 VARCHAR(64),  --  order_id
    column2 VARCHAR(64),  --  customer_id
    column3 VARCHAR(12),  --  order_status
    column4 TIMESTAMP,    --  order_purchase_timestamp
    column5 TIMESTAMP,    --  order_approved_at
    column6 TIMESTAMP,    --  order_delivered_carrier_date
    column7 TIMESTAMP,
    column8 TIMESTAMP     --  order_estimated_delivery_date
);

\COPY temp_order_table FROM '/Users/xavierd/Downloads/dataset/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;

INSERT INTO order_table (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
SELECT column1, column2, column3, column4, column5, column6, column7, column8 FROM temp_order_table;

DROP TABLE temp_order_table;

-- populate values into the review_table
CREATE TEMPORARY TABLE temp_review_table (
    column1 VARCHAR(64),  --  review_id
    column2 VARCHAR(64),  --  order_id
    column3 INT,          --  review_score
    column4 VARCHAR(255), --  review_comment_title
    column5 TEXT,         --  review_comment_message
    column6 TIMESTAMP,    --  review_creation_date
    column7 TIMESTAMP     --  review_answer_timestamp
);

\COPY temp_review_table FROM '/Users/xavierd/Downloads/dataset/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;

INSERT INTO review_table (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
SELECT column1, column2, column3, column4, column5, column6, column7 FROM temp_review_table;

DROP TABLE temp_review_table;

-- populate values into the payment_table
CREATE TEMPORARY TABLE temp_payment_table (
    column1 VARCHAR(64),  --  order_id
    column2 INT,          --  payment_sequencial
    column3 VARCHAR(50),  --  payment_type
    column4 INT,          --  payment_installments
    column5 FLOAT         --  payment_value
);

\COPY temp_payment_table FROM '/Users/xavierd/Downloads/dataset/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

INSERT INTO payment_table (order_id, payment_sequencial, payment_type, payment_installments, payment_value)
SELECT column1, column2, column3, column4, column5 FROM temp_payment_table;

DROP TABLE temp_payment_table;

-- populate values into the product_table
CREATE TEMPORARY TABLE temp_product_table (
    column1 VARCHAR(64),  --  product_id
    column2 VARCHAR(100), --  product_category_name
    column3 INT,          --  product_name_length
    column4 INT,          --  product_description_length
    column5 INT,          --  product_photos_qty
    column6 FLOAT,        --  product_weight_g
    column7 FLOAT,        --  product_length_cm
    column8 FLOAT,        --  product_height_cm
    column9 FLOAT         --  product_width_cm
);

\COPY temp_product_table FROM '/Users/xavierd/Downloads/dataset/olist_products_dataset.csv' DELIMITER ',' CSV HEADER;

INSERT INTO product_table (
    product_id, 
    product_category_name, 
    product_name_length, 
    product_description_length, 
    product_photos_qty, 
    product_weight_g, 
    product_length_cm, 
    product_height_cm, 
    product_width_cm
)
SELECT 
    column1, 
    column2, 
    column3, 
    column4, 
    column5, 
    column6, 
    column7, 
    column8, 
    column9 
FROM temp_product_table;

DROP TABLE temp_product_table;

-- populate values into the order_item
-- Create Temporary Table
CREATE TEMPORARY TABLE temp_order_item_table(
    order_id VARCHAR(64),
    order_item_id INT,
    product_id VARCHAR(64),
    seller_id VARCHAR(64),
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2)
);

-- Import data from CSV Files
\COPY temp_order_item_table FROM '/Users/xavierd/Downloads/dataset/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;

-- DELETE FROM temp_order_item_table
-- WHERE ctid IN (
--     SELECT ctid
--     FROM (
--         SELECT ctid,
--         ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY ctid DESC) as row_num
--         FROM temp_order_item_table
--     ) t
--     WHERE t.row_num > 1
-- );

-- Insert data to Order_Item Table
INSERT INTO Order_Item (order_id, product_id, order_item_id, seller_id, shipping_limit_date, price, freight_value)
SELECT order_id, product_id, order_item_id, seller_id, shipping_limit_date, price, freight_value
FROM temp_order_item_table;

-- Delete Temporary Table
DROP TABLE temp_order_item_table;

-- create temporary table for category_translation
CREATE TEMPORARY TABLE temp_category_translation_table(
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);

-- import data from CSV file into temporary table
\COPY temp_category_translation_table FROM '/Users/xavierd/Downloads/dataset/product_category_name_translation.csv' DELIMITER ',' CSV HEADER;

-- insert data from temporary table into Category_Translation table
INSERT INTO Category_Translation (product_category_name, product_category_name_english)
SELECT product_category_name, product_category_name_english FROM temp_category_translation_table;

-- drop temporary table
DROP TABLE temp_category_translation_table;
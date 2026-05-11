-- PostgreSQL seed data for E2E tests
-- Covers: String, Int, Float, Boolean, Timestamp, JSON, Geometry (PostGIS)

CREATE EXTENSION IF NOT EXISTS postgis;

-- Categories (self-referential)
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE,
    parent_id INTEGER REFERENCES categories(id)
);

INSERT INTO categories (id, name, parent_id) VALUES
    (1, 'Electronics', NULL),
    (2, 'Computers', 1),
    (3, 'Accessories', 1);

SELECT setval('categories_id_seq', 3);

-- Tags
CREATE TABLE tags (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL UNIQUE
);

INSERT INTO tags (id, name) VALUES
    (1, 'sale'),
    (2, 'new'),
    (3, 'popular');

SELECT setval('tags_id_seq', 3);

-- Products
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    tags JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ,
    description TEXT,
    category_id INTEGER REFERENCES categories(id)
);

INSERT INTO products (id, name, price, quantity, is_active, tags, description, category_id) VALUES
    (1, 'Laptop Pro', 1299.99, 10, true, '{"color": "silver", "weight": 1.5}', 'High-performance laptop', 2),
    (2, 'Wireless Mouse', 29.99, 50, true, '{"color": "black"}', 'Ergonomic wireless mouse', 3),
    (3, 'USB-C Hub', 49.99, 30, true, NULL, 'Multi-port USB-C hub', 3),
    (4, 'Old Keyboard', 19.99, 0, false, '{"color": "white"}', 'Discontinued keyboard', 3),
    (5, 'Gaming Desktop', 2499.99, 5, true, '{"color": "black", "rgb": true}', 'High-end gaming desktop', 2);

SELECT setval('products_id_seq', 5);

-- Product-Tags M2M
CREATE TABLE product_tags (
    product_id INTEGER REFERENCES products(id),
    tag_id INTEGER REFERENCES tags(id),
    PRIMARY KEY (product_id, tag_id)
);

INSERT INTO product_tags (product_id, tag_id) VALUES
    (1, 2),  -- Laptop Pro: new
    (1, 3),  -- Laptop Pro: popular
    (2, 1),  -- Wireless Mouse: sale
    (2, 3),  -- Wireless Mouse: popular
    (5, 2),  -- Gaming Desktop: new
    (5, 3);  -- Gaming Desktop: popular

-- Locations (PostGIS geometry)
CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    point GEOMETRY(Point, 4326),
    area GEOMETRY(Polygon, 4326)
);

INSERT INTO locations (id, name, point, area) VALUES
    (1, 'Store A', ST_SetSRID(ST_MakePoint(-73.935242, 40.730610), 4326),
     ST_SetSRID(ST_MakeEnvelope(-73.94, 40.73, -73.93, 40.74), 4326)),
    (2, 'Store B', ST_SetSRID(ST_MakePoint(-118.243685, 34.052234), 4326),
     ST_SetSRID(ST_MakeEnvelope(-118.25, 34.05, -118.24, 34.06), 4326));

SELECT setval('locations_id_seq', 2);

-- Product details (JSONB structural type)
CREATE TABLE product_details (
    product_id INTEGER PRIMARY KEY REFERENCES products(id),
    specs JSONB,
    metadata JSONB
);

INSERT INTO product_details VALUES
    (1, '{"cpu": "M2", "ram_gb": 16, "storage_gb": 512}', '{"warranty_years": 2, "origin": "US"}'),
    (2, '{"dpi": 1600, "buttons": 5, "wireless": true}', '{"warranty_years": 1, "origin": "CN"}'),
    (5, '{"cpu": "i9", "ram_gb": 64, "storage_gb": 2000}', '{"warranty_years": 3, "origin": "US"}');

-- Price ranges (range types)
CREATE TABLE price_ranges (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id),
    valid_price INT4RANGE,
    valid_period TSTZRANGE
);

INSERT INTO price_ranges VALUES
    (1, 1, '[1000, 1500)', '[2025-01-01, 2025-06-30)'),
    (2, 2, '[20, 40)', '[2025-03-01, 2025-12-31)'),
    (3, 5, '[2000, 3000)', '[2025-01-01, 2025-12-31)');

SELECT setval('price_ranges_id_seq', 3);

-- json_field_demo: dataset for JSONFieldFilter coverage (path / isNull / coalesce / typed sub-filters)
CREATE TABLE json_field_demo (
    id INTEGER PRIMARY KEY,
    data JSONB
);

INSERT INTO json_field_demo (id, data) VALUES
    (1,  '{"user":{"age":31,"country":"DE"},"metrics":{"score":0.92},"shape":{"type":"Point","coordinates":[10,51]},"event":{"at":"2024-06-01T10:00:00Z"}}'),
    (2,  '{"user":{"age":65,"country":"FR"},"metrics":{"score":0.50},"shape":{"type":"Point","coordinates":[2,48]},"event":{"at":"2024-06-02T11:00:00Z"}}'),
    (3,  '{"user":{"age":14,"country":"DE"},"metrics":{"score":0.10},"shape":{"type":"Point","coordinates":[11,52]},"event":{"at":"2024-06-03T09:00:00Z"}}'),
    (4,  '{"user":{"age":null,"country":"DE"},"metrics":{"score":null},"shape":null,"event":{"at":null}}'),
    (5,  '{"user":{"country":"FR"},"metrics":{"score":0.75},"event":{"at":"2024-06-05T12:00:00Z"}}'),
    (6,  NULL),
    (7,  '{}'::jsonb),
    (8,  '{"user":{"age":17,"country":"DE"},"metrics":{"score":0.65},"shape":{"type":"Point","coordinates":[10.1,51.1]},"event":{"at":"2024-06-08T08:00:00Z"}}'),
    (9,  '{"user":{"age":40,"country":"DE"},"metrics":{"score":0.20},"shape":{"type":"Point","coordinates":[12,53]},"event":{"at":"2024-06-09T14:00:00Z"}}'),
    (10, '{"user":{"age":18,"country":"FR"},"metrics":{"score":0.50},"shape":{"type":"Point","coordinates":[2.5,48.5]},"event":{"at":"2024-06-10T15:00:00Z"}}'),
    -- Rows 11-12 carry extra paths for typed sub-filter coverage
    -- (bigInt, float, bool, date, time, dateTime, interval, timestamp).
    (11, '{"user":{"age":35,"country":"US"},"metrics":{"score":0.80},"shape":{"type":"Point","coordinates":[10.5,51.5]},"event":{"at":"2024-06-11T10:00:00Z","local_dt":"2024-06-11T10:00:00"},"account":{"balance":5000000000},"flags":{"premium":true},"signup":{"day":"2024-01-15"},"lunch":{"at_time":"12:30:00"},"subscription":{"duration":"01:30:00"}}'),
    (12, '{"user":{"age":22,"country":"US"},"metrics":{"score":0.40},"shape":{"type":"Point","coordinates":[10.6,51.6]},"event":{"at":"2024-06-12T10:00:00Z","local_dt":"2024-06-12T10:00:00"},"account":{"balance":2500000000},"flags":{"premium":false},"signup":{"day":"2024-02-20"},"lunch":{"at_time":"13:00:00"},"subscription":{"duration":"02:00:00"}}');

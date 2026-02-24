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
    tags JSON,
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

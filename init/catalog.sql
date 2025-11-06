CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name TEXT,
  price NUMERIC
);

CREATE TABLE categories (
  id SERIAL PRIMARY KEY,
  name TEXT
);

ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE categories REPLICA IDENTITY FULL;

INSERT INTO categories (name) VALUES
('Electronics'), ('Clothing');

INSERT INTO products (name, price) VALUES
('Smartphone', 799.99),
('T-Shirt', 15.99);

CREATE TABLE shipments (
  id SERIAL PRIMARY KEY,
  order_id INT,
  shipped_at TIMESTAMP,
  status TEXT
);

CREATE TABLE drivers (
  id SERIAL PRIMARY KEY,
  name TEXT
);

ALTER TABLE shipments REPLICA IDENTITY FULL;
ALTER TABLE drivers REPLICA IDENTITY FULL;

INSERT INTO drivers (name) VALUES
('John Doe'), ('Jane Smith');

INSERT INTO shipments (order_id, shipped_at, status) VALUES
(1, NOW(), 'shipped'),
(2, NOW(), 'pending');
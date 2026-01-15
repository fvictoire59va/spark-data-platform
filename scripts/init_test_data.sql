-- Initialisation des données de test pour PostgreSQL
-- Script à exécuter dans le container PostgreSQL

-- Créer le schéma sales s'il n'existe pas
CREATE SCHEMA IF NOT EXISTS sales;

-- Créer la table orders
DROP TABLE IF EXISTS sales.orders;
CREATE TABLE sales.orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_datetime TIMESTAMP NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount DECIMAL(10, 2) DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insérer les données de test
INSERT INTO sales.orders (order_id, customer_id, order_datetime, product_id, quantity, unit_price, discount, status) 
VALUES 
    ('ORD-20260115-001', 'CUST-001', '2026-01-15 08:00:00', 'PROD-A', 2, 49.99, 0.00, 'COMPLETED'),
    ('ORD-20260115-002', 'CUST-002', '2026-01-15 09:30:00', 'PROD-B', 1, 99.99, 10.00, 'COMPLETED'),
    ('ORD-20260115-003', 'CUST-001', '2026-01-15 10:15:00', 'PROD-C', 5, 19.99, 0.00, 'PENDING'),
    ('ORD-20260115-004', 'CUST-003', '2026-01-15 11:45:00', 'PROD-A', 3, 49.99, 5.00, 'COMPLETED'),
    ('ORD-20260115-005', 'CUST-004', '2026-01-15 13:20:00', 'PROD-D', 1, 149.99, 15.00, 'PENDING'),
    ('ORD-20260115-006', 'CUST-002', '2026-01-15 14:00:00', 'PROD-B', 2, 99.99, 0.00, 'CANCELLED'),
    ('ORD-20260115-007', 'CUST-005', '2026-01-15 15:30:00', 'PROD-E', 4, 29.99, 2.00, 'COMPLETED'),
    ('ORD-20260115-008', 'CUST-003', '2026-01-15 16:45:00', 'PROD-A', 1, 49.99, 0.00, 'COMPLETED');

-- Créer la table customers
DROP TABLE IF EXISTS sales.customers;
CREATE TABLE sales.customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insérer les données clients
INSERT INTO sales.customers (customer_id, customer_name, email, country)
VALUES
    ('CUST-001', 'Alice Johnson', 'alice@example.com', 'USA'),
    ('CUST-002', 'Bob Smith', 'bob@example.com', 'UK'),
    ('CUST-003', 'Charlie Brown', 'charlie@example.com', 'USA'),
    ('CUST-004', 'Diana Prince', 'diana@example.com', 'Canada'),
    ('CUST-005', 'Eve Davis', 'eve@example.com', 'Australia');

-- Vérifier les données
SELECT COUNT(*) as total_orders FROM sales.orders;
SELECT COUNT(*) as total_customers FROM sales.customers;

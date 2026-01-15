"""Script pour initialiser les données de test dans PostgreSQL."""

import sys

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def init_postgres_data() -> None:
    """Initialiser les données de test dans PostgreSQL."""
    # Configuration de connexion
    config = {
        "host": "localhost",
        "port": "5432",
        "user": "hive",
        "password": "hive123",
        "database": "metastore",
    }

    try:
        # Connexion à PostgreSQL
        print("🔗 Connexion à PostgreSQL...")
        conn = psycopg2.connect(**config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Créer la base de données dev_db
        print("📁 Création de la base de données dev_db...")
        cursor.execute("DROP DATABASE IF EXISTS dev_db;")
        cursor.execute("CREATE DATABASE dev_db;")
        print("✅ Base de données dev_db créée")

        # Se connecter à dev_db
        config["database"] = "dev_db"
        conn.close()
        conn = psycopg2.connect(**config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Créer le schéma sales
        print("📊 Création du schéma sales...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS sales;")

        # Créer la table orders
        print("📋 Création de la table sales.orders...")
        cursor.execute(
            """
            DROP TABLE IF EXISTS sales.orders CASCADE;
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
        """
        )

        # Insérer les données de test
        print("📥 Insertion des données de commandes...")
        cursor.execute(
            """
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
        """
        )

        # Créer la table customers
        print("👥 Création de la table sales.customers...")
        cursor.execute(
            """
            DROP TABLE IF EXISTS sales.customers CASCADE;
            CREATE TABLE sales.customers (
                customer_id VARCHAR(50) PRIMARY KEY,
                customer_name VARCHAR(100) NOT NULL,
                email VARCHAR(100),
                country VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        )

        # Insérer les données clients
        print("📥 Insertion des données de clients...")
        cursor.execute(
            """
            INSERT INTO sales.customers (customer_id, customer_name, email, country)
            VALUES
                ('CUST-001', 'Alice Johnson', 'alice@example.com', 'USA'),
                ('CUST-002', 'Bob Smith', 'bob@example.com', 'UK'),
                ('CUST-003', 'Charlie Brown', 'charlie@example.com', 'USA'),
                ('CUST-004', 'Diana Prince', 'diana@example.com', 'Canada'),
                ('CUST-005', 'Eve Davis', 'eve@example.com', 'Australia');
        """
        )

        # Vérifier les données
        cursor.execute("SELECT COUNT(*) as total_orders FROM sales.orders;")
        orders_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) as total_customers FROM sales.customers;")
        customers_count = cursor.fetchone()[0]

        print("\n✅ Initialisation réussie !")
        print(f"   📦 Total commandes: {orders_count}")
        print(f"   👥 Total clients: {customers_count}")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"❌ Erreur PostgreSQL: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Erreur: {e}")
        sys.exit(1)


if __name__ == "__main__":
    init_postgres_data()

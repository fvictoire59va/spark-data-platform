# tests/fixtures/sample_data.py
"""Données de test pour les tests unitaires et d'intégration."""

from __future__ import annotations

from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Any
import random
import string
import uuid


# =============================================================================
# DONNÉES CLIENTS
# =============================================================================

SAMPLE_CUSTOMERS_VALID = [
    {
        "customer_id": "CUST001",
        "email": "john.doe@example.com",
        "first_name": "John",
        "last_name": "Doe",
        "phone": "+33612345678",
        "birth_date": "1985-03-15",
        "gender": "M",
        "address": "123 Main Street",
        "city": "Paris",
        "postal_code": "75001",
        "country": "FR",
        "registration_date": "2020-01-15",
        "is_active": True,
        "customer_segment": "PREMIUM",
    },
    {
        "customer_id": "CUST002",
        "email": "jane.smith@example.com",
        "first_name": "Jane",
        "last_name": "Smith",
        "phone": "+33698765432",
        "birth_date": "1990-07-22",
        "gender": "F",
        "address": "456 Oak Avenue",
        "city": "Lyon",
        "postal_code": "69001",
        "country": "FR",
        "registration_date": "2021-06-20",
        "is_active": True,
        "customer_segment": "STANDARD",
    },
    {
        "customer_id": "CUST003",
        "email": "bob.wilson@example.com",
        "first_name": "Bob",
        "last_name": "Wilson",
        "phone": "+33611223344",
        "birth_date": "1978-11-08",
        "gender": "M",
        "address": "789 Pine Road",
        "city": "Marseille",
        "postal_code": "13001",
        "country": "FR",
        "registration_date": "2019-03-10",
        "is_active": True,
        "customer_segment": "VIP",
    },
    {
        "customer_id": "CUST004",
        "email": "alice.martin@example.com",
        "first_name": "Alice",
        "last_name": "Martin",
        "phone": "+33655667788",
        "birth_date": "1995-02-28",
        "gender": "F",
        "address": "321 Elm Street",
        "city": "Toulouse",
        "postal_code": "31000",
        "country": "FR",
        "registration_date": "2022-09-05",
        "is_active": True,
        "customer_segment": "STANDARD",
    },
    {
        "customer_id": "CUST005",
        "email": "charlie.brown@example.com",
        "first_name": "Charlie",
        "last_name": "Brown",
        "phone": "+33699887766",
        "birth_date": "1982-12-10",
        "gender": "M",
        "address": "654 Maple Drive",
        "city": "Nice",
        "postal_code": "06000",
        "country": "FR",
        "registration_date": "2018-11-25",
        "is_active": False,
        "customer_segment": "ENTERPRISE",
    },
]

SAMPLE_CUSTOMERS_INVALID = [
    {
        # Email invalide
        "customer_id": "CUST_INV001",
        "email": "invalid-email",
        "first_name": "Invalid",
        "last_name": "Email",
        "phone": "+33612345678",
        "birth_date": "1985-03-15",
        "gender": "M",
        "city": "Paris",
        "country": "FR",
        "registration_date": "2020-01-15",
        "is_active": True,
        "customer_segment": "STANDARD",
    },
    {
        # customer_id manquant
        "customer_id": None,
        "email": "no.id@example.com",
        "first_name": "No",
        "last_name": "ID",
        "phone": "+33612345678",
        "birth_date": "1985-03-15",
        "gender": "M",
        "city": "Paris",
        "country": "FR",
        "registration_date": "2020-01-15",
        "is_active": True,
        "customer_segment": "STANDARD",
    },
    {
        # Date de naissance dans le futur
        "customer_id": "CUST_INV003",
        "email": "future.birth@example.com",
        "first_name": "Future",
        "last_name": "Birth",
        "phone": "+33612345678",
        "birth_date": "2050-01-01",
        "gender": "M",
        "city": "Paris",
        "country": "FR",
        "registration_date": "2020-01-15",
        "is_active": True,
        "customer_segment": "STANDARD",
    },
    {
        # Données minimales (beaucoup de nulls)
        "customer_id": "CUST_INV004",
        "email": None,
        "first_name": None,
        "last_name": None,
        "phone": None,
        "birth_date": None,
        "gender": None,
        "city": None,
        "country": None,
        "registration_date": None,
        "is_active": None,
        "customer_segment": None,
    },
]

SAMPLE_CUSTOMERS_DUPLICATES = [
    {
        "customer_id": "CUST001",
        "email": "john.doe@example.com",
        "first_name": "John",
        "last_name": "Doe",
        "city": "Paris",
        "country": "FR",
        "registration_date": "2020-01-15",
        "is_active": True,
    },
    {
        # Doublon avec même customer_id
        "customer_id": "CUST001",
        "email": "john.doe.dup@example.com",
        "first_name": "John",
        "last_name": "Doe",
        "city": "Paris",
        "country": "FR",
        "registration_date": "2020-01-16",
        "is_active": True,
    },
    {
        "customer_id": "CUST002",
        "email": "jane.smith@example.com",
        "first_name": "Jane",
        "last_name": "Smith",
        "city": "Lyon",
        "country": "FR",
        "registration_date": "2021-06-20",
        "is_active": True,
    },
]


# =============================================================================
# DONNÉES PRODUITS
# =============================================================================

SAMPLE_PRODUCTS_VALID = [
    {
        "product_id": "PROD001",
        "product_name": "Laptop Pro 15",
        "description": "High-performance laptop with 15-inch display",
        "category": "Electronics",
        "subcategory": "Computers",
        "brand": "TechBrand",
        "price": 1299.99,
        "cost": 850.00,
        "stock_quantity": 150,
        "is_active": True,
        "created_date": "2022-01-10",
        "updated_date": "2023-06-15",
    },
    {
        "product_id": "PROD002",
        "product_name": "Wireless Mouse",
        "description": "Ergonomic wireless mouse with long battery life",
        "category": "Electronics",
        "subcategory": "Accessories",
        "brand": "TechBrand",
        "price": 49.99,
        "cost": 20.00,
        "stock_quantity": 500,
        "is_active": True,
        "created_date": "2021-05-20",
        "updated_date": "2023-03-10",
    },
    {
        "product_id": "PROD003",
        "product_name": "Office Chair Deluxe",
        "description": "Comfortable office chair with lumbar support",
        "category": "Furniture",
        "subcategory": "Chairs",
        "brand": "ComfortPlus",
        "price": 349.99,
        "cost": 180.00,
        "stock_quantity": 75,
        "is_active": True,
        "created_date": "2022-08-15",
        "updated_date": "2023-01-20",
    },
    {
        "product_id": "PROD004",
        "product_name": "USB-C Hub",
        "description": "Multi-port USB-C hub with HDMI output",
        "category": "Electronics",
        "subcategory": "Accessories",
        "brand": "ConnectPro",
        "price": 79.99,
        "cost": 35.00,
        "stock_quantity": 300,
        "is_active": True,
        "created_date": "2023-02-01",
        "updated_date": "2023-07-10",
    },
    {
        "product_id": "PROD005",
        "product_name": "Standing Desk",
        "description": "Electric height-adjustable standing desk",
        "category": "Furniture",
        "subcategory": "Desks",
        "brand": "ErgoWork",
        "price": 599.99,
        "cost": 320.00,
        "stock_quantity": 40,
        "is_active": True,
        "created_date": "2022-11-01",
        "updated_date": "2023-05-25",
    },
    {
        "product_id": "PROD006",
        "product_name": "Mechanical Keyboard",
        "description": "RGB mechanical keyboard with Cherry MX switches",
        "category": "Electronics",
        "subcategory": "Accessories",
        "brand": "KeyMaster",
        "price": 149.99,
        "cost": 70.00,
        "stock_quantity": 200,
        "is_active": True,
        "created_date": "2022-04-10",
        "updated_date": "2023-08-01",
    },
]

SAMPLE_PRODUCTS_INVALID = [
    {
        # Prix négatif
        "product_id": "PROD_INV001",
        "product_name": "Invalid Price Product",
        "category": "Electronics",
        "price": -50.00,
        "stock_quantity": 100,
        "is_active": True,
    },
    {
        # Stock négatif
        "product_id": "PROD_INV002",
        "product_name": "Negative Stock Product",
        "category": "Electronics",
        "price": 99.99,
        "stock_quantity": -10,
        "is_active": True,
    },
    {
        # product_id manquant
        "product_id": None,
        "product_name": "No ID Product",
        "category": "Electronics",
        "price": 99.99,
        "stock_quantity": 100,
        "is_active": True,
    },
]


# =============================================================================
# DONNÉES TRANSACTIONS
# =============================================================================

SAMPLE_TRANSACTIONS_VALID = [
    {
        "transaction_id": "TXN001",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 1,
        "unit_price": 1299.99,
        "discount": 0.0,
        "total_amount": 1299.99,
        "transaction_date": "2023-07-15 10:30:00",
        "payment_method": "CREDIT_CARD",
        "status": "COMPLETED",
        "channel": "WEB",
    },
    {
        "transaction_id": "TXN002",
        "customer_id": "CUST002",
        "product_id": "PROD002",
        "quantity": 2,
        "unit_price": 49.99,
        "discount": 5.0,
        "total_amount": 94.98,
        "transaction_date": "2023-07-15 14:45:00",
        "payment_method": "DEBIT_CARD",
        "status": "COMPLETED",
        "channel": "STORE",
    },
    {
        "transaction_id": "TXN003",
        "customer_id": "CUST003",
        "product_id": "PROD003",
        "quantity": 1,
        "unit_price": 349.99,
        "discount": 0.0,
        "total_amount": 349.99,
        "transaction_date": "2023-07-16 09:15:00",
        "payment_method": "BANK_TRANSFER",
        "status": "COMPLETED",
        "channel": "WEB",
    },
    {
        "transaction_id": "TXN004",
        "customer_id": "CUST001",
        "product_id": "PROD004",
        "quantity": 3,
        "unit_price": 79.99,
        "discount": 10.0,
        "total_amount": 229.97,
        "transaction_date": "2023-07-17 16:20:00",
        "payment_method": "CREDIT_CARD",
        "status": "COMPLETED",
        "channel": "MOBILE",
    },
    {
        "transaction_id": "TXN005",
        "customer_id": "CUST004",
        "product_id": "PROD005",
        "quantity": 1,
        "unit_price": 599.99,
        "discount": 50.0,
        "total_amount": 549.99,
        "transaction_date": "2023-07-18 11:00:00",
        "payment_method": "CREDIT_CARD",
        "status": "COMPLETED",
        "channel": "WEB",
    },
    {
        "transaction_id": "TXN006",
        "customer_id": "CUST002",
        "product_id": "PROD006",
        "quantity": 1,
        "unit_price": 149.99,
        "discount": 0.0,
        "total_amount": 149.99,
        "transaction_date": "2023-07-19 13:30:00",
        "payment_method": "PAYPAL",
        "status": "COMPLETED",
        "channel": "WEB",
    },
    {
        "transaction_id": "TXN007",
        "customer_id": "CUST005",
        "product_id": "PROD001",
        "quantity": 2,
        "unit_price": 1299.99,
        "discount": 100.0,
        "total_amount": 2499.98,
        "transaction_date": "2023-07-20 10:00:00",
        "payment_method": "BANK_TRANSFER",
        "status": "COMPLETED",
        "channel": "STORE",
    },
    {
        "transaction_id": "TXN008",
        "customer_id": "CUST003",
        "product_id": "PROD002",
        "quantity": 5,
        "unit_price": 49.99,
        "discount": 25.0,
        "total_amount": 224.95,
        "transaction_date": "2023-07-21 15:45:00",
        "payment_method": "CREDIT_CARD",
        "status": "PENDING",
        "channel": "WEB",
    },
]

SAMPLE_TRANSACTIONS_INVALID = [
    {
        # Quantité négative
        "transaction_id": "TXN_INV001",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": -1,
        "unit_price": 100.0,
        "total_amount": -100.0,
        "transaction_date": "2023-07-15 10:30:00",
        "payment_method": "CREDIT_CARD",
        "status": "COMPLETED",
    },
    {
        # Montant incohérent
        "transaction_id": "TXN_INV002",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 2,
        "unit_price": 100.0,
        "discount": 0.0,
        "total_amount": 500.0,  # Devrait être 200.0
        "transaction_date": "2023-07-15 10:30:00",
        "payment_method": "CREDIT_CARD",
        "status": "COMPLETED",
    },
    {
        # Customer inexistant
        "transaction_id": "TXN_INV003",
        "customer_id": "CUST_UNKNOWN",
        "product_id": "PROD001",
        "quantity": 1,
        "unit_price": 100.0,
        "total_amount": 100.0,
        "transaction_date": "2023-07-15 10:30:00",
        "payment_method": "CREDIT_CARD",
        "status": "COMPLETED",
    },
    {
        # Date dans le futur
        "transaction_id": "TXN_INV004",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 1,
        "unit_price": 100.0,
        "total_amount": 100.0,
        "transaction_date": "2050-01-01 10:30:00",
        "payment_method": "CREDIT_CARD",
        "status": "COMPLETED",
    },
]

SAMPLE_TRANSACTIONS_REFUNDS = [
    {
        "transaction_id": "TXN_REF001",
        "customer_id": "CUST001",
        "product_id": "PROD001",
        "quantity": 1,
        "unit_price": 1299.99,
        "discount": 0.0,
        "total_amount": -1299.99,
        "transaction_date": "2023-07-20 10:30:00",
        "payment_method": "CREDIT_CARD",
        "status": "REFUNDED",
        "channel": "WEB",
        "original_transaction_id": "TXN001",
    },
]


# =============================================================================
# DONNÉES COMMANDES (ORDERS)
# =============================================================================

SAMPLE_ORDERS_VALID = [
    {
        "order_id": "ORD001",
        "customer_id": "CUST001",
        "order_date": "2023-07-15",
        "status": "DELIVERED",
        "shipping_address": "123 Main Street, Paris 75001",
        "billing_address": "123 Main Street, Paris 75001",
        "subtotal": 1349.98,
        "shipping_cost": 15.00,
        "tax": 270.00,
        "total_amount": 1634.98,
        "payment_method": "CREDIT_CARD",
        "payment_status": "PAID",
    },
    {
        "order_id": "ORD002",
        "customer_id": "CUST002",
        "order_date": "2023-07-16",
        "status": "SHIPPED",
        "shipping_address": "456 Oak Avenue, Lyon 69001",
        "billing_address": "456 Oak Avenue, Lyon 69001",
        "subtotal": 199.98,
        "shipping_cost": 10.00,
        "tax": 40.00,
        "total_amount": 249.98,
        "payment_method": "PAYPAL",
        "payment_status": "PAID",
    },
    {
        "order_id": "ORD003",
        "customer_id": "CUST003",
        "order_date": "2023-07-17",
        "status": "PROCESSING",
        "shipping_address": "789 Pine Road, Marseille 13001",
        "billing_address": "789 Pine Road, Marseille 13001",
        "subtotal": 599.99,
        "shipping_cost": 0.00,
        "tax": 120.00,
        "total_amount": 719.99,
        "payment_method": "BANK_TRANSFER",
        "payment_status": "PENDING",
    },
]

SAMPLE_ORDER_ITEMS = [
    # Order 1
    {"order_id": "ORD001", "product_id": "PROD001", "quantity": 1, "unit_price": 1299.99, "line_total": 1299.99},
    {"order_id": "ORD001", "product_id": "PROD002", "quantity": 1, "unit_price": 49.99, "line_total": 49.99},
    # Order 2
    {"order_id": "ORD002", "product_id": "PROD002", "quantity": 2, "unit_price": 49.99, "line_total": 99.98},
    {"order_id": "ORD002", "product_id": "PROD004", "quantity": 1, "unit_price": 79.99, "line_total": 79.99},
    {"order_id": "ORD002", "product_id": "PROD006", "quantity": 1, "unit_price": 20.00, "line_total": 20.00},
    # Order 3
    {"order_id": "ORD003", "product_id": "PROD005", "quantity": 1, "unit_price": 599.99, "line_total": 599.99},
]


# =============================================================================
# GÉNÉRATEURS DE DONNÉES
# =============================================================================

class DataGenerator:
    """Générateur de données de test."""
    
    FIRST_NAMES = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]
    LAST_NAMES = ["Doe", "Smith", "Wilson", "Martin", "Brown", "Taylor", "Anderson", "Thomas", "Jackson", "White"]
    CITIES = ["Paris", "Lyon", "Marseille", "Toulouse", "Nice", "Nantes", "Strasbourg", "Bordeaux", "Lille", "Rennes"]
    COUNTRIES = ["FR", "BE", "CH", "DE", "ES", "IT", "UK", "US", "CA", "JP"]
    SEGMENTS = ["STANDARD", "PREMIUM", "VIP", "ENTERPRISE"]
    CATEGORIES = ["Electronics", "Furniture", "Clothing", "Food", "Books", "Sports", "Beauty", "Home"]
    PAYMENT_METHODS = ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER", "CASH"]
    CHANNELS = ["WEB", "MOBILE", "STORE", "PHONE"]
    
    @classmethod
    def generate_customer_id(cls) -> str:
        """Génère un ID client unique."""
        return f"CUST{uuid.uuid4().hex[:8].upper()}"
    
    @classmethod
    def generate_product_id(cls) -> str:
        """Génère un ID produit unique."""
        return f"PROD{uuid.uuid4().hex[:8].upper()}"
    
    @classmethod
    def generate_transaction_id(cls) -> str:
        """Génère un ID transaction unique."""
        return f"TXN{uuid.uuid4().hex[:12].upper()}"
    
    @classmethod
    def generate_order_id(cls) -> str:
        """Génère un ID commande unique."""
        return f"ORD{uuid.uuid4().hex[:8].upper()}"
    
    @classmethod
    def generate_email(cls, first_name: str, last_name: str) -> str:
        """Génère un email à partir du nom."""
        domain = random.choice(["example.com", "test.com", "mail.com", "company.org"])
        return f"{first_name.lower()}.{last_name.lower()}@{domain}"
    
    @classmethod
    def generate_phone(cls, country: str = "FR") -> str:
        """Génère un numéro de téléphone."""
        country_codes = {"FR": "+33", "BE": "+32", "CH": "+41", "DE": "+49", "US": "+1"}
        prefix = country_codes.get(country, "+33")
        number = "".join(random.choices(string.digits, k=9))
        return f"{prefix}{number}"
    
    @classmethod
    def generate_date(
        cls,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> str:
        """Génère une date aléatoire."""
        start = start_date or date(2020, 1, 1)
        end = end_date or date.today()
        delta = (end - start).days
        random_days = random.randint(0, delta)
        result = start + timedelta(days=random_days)
        return result.strftime("%Y-%m-%d")
    
    @classmethod
    def generate_datetime(
        cls,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> str:
        """Génère un datetime aléatoire."""
        start = start_date or datetime(2023, 1, 1)
        end = end_date or datetime.now()
        delta = (end - start).total_seconds()
        random_seconds = random.randint(0, int(delta))
        result = start + timedelta(seconds=random_seconds)
        return result.strftime("%Y-%m-%d %H:%M:%S")
    
    @classmethod
    def generate_customer(cls, **overrides) -> dict[str, Any]:
        """Génère un client aléatoire."""
        first_name = random.choice(cls.FIRST_NAMES)
        last_name = random.choice(cls.LAST_NAMES)
        country = random.choice(cls.COUNTRIES)
        
        customer = {
            "customer_id": cls.generate_customer_id(),
            "email": cls.generate_email(first_name, last_name),
            "first_name": first_name,
            "last_name": last_name,
            "phone": cls.generate_phone(country),
            "birth_date": cls.generate_date(date(1950, 1, 1), date(2005, 1, 1)),
            "gender": random.choice(["M", "F"]),
            "city": random.choice(cls.CITIES),
            "country": country,
            "registration_date": cls.generate_date(date(2018, 1, 1), date.today()),
            "is_active": random.choice([True, True, True, False]),
            "customer_segment": random.choice(cls.SEGMENTS),
        }
        customer.update(overrides)
        return customer
    
    @classmethod
    def generate_customers(cls, count: int, **overrides) -> list[dict[str, Any]]:
        """Génère plusieurs clients."""
        return [cls.generate_customer(**overrides) for _ in range(count)]
    
    @classmethod
    def generate_product(cls, **overrides) -> dict[str, Any]:
        """Génère un produit aléatoire."""
        category = random.choice(cls.CATEGORIES)
        price = round(random.uniform(10.0, 2000.0), 2)
        cost = round(price * random.uniform(0.3, 0.7), 2)
        
        product = {
            "product_id": cls.generate_product_id(),
            "product_name": f"{category} Product {random.randint(100, 999)}",
            "description": f"A great {category.lower()} product",
            "category": category,
            "brand": f"Brand{random.choice(string.ascii_uppercase)}",
            "price": price,
            "cost": cost,
            "stock_quantity": random.randint(0, 1000),
            "is_active": random.choice([True, True, True, False]),
            "created_date": cls.generate_date(date(2020, 1, 1), date(2023, 1, 1)),
            "updated_date": cls.generate_date(date(2023, 1, 1), date.today()),
        }
        product.update(overrides)
        return product
    
    @classmethod
    def generate_products(cls, count: int, **overrides) -> list[dict[str, Any]]:
        """Génère plusieurs produits."""
        return [cls.generate_product(**overrides) for _ in range(count)]
    
    @classmethod
    def generate_transaction(
        cls,
        customer_ids: list[str] | None = None,
        product_ids: list[str] | None = None,
        **overrides,
    ) -> dict[str, Any]:
        """Génère une transaction aléatoire."""
        customer_id = random.choice(customer_ids) if customer_ids else cls.generate_customer_id()
        product_id = random.choice(product_ids) if product_ids else cls.generate_product_id()
        
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(10.0, 500.0), 2)
        discount = round(random.uniform(0, unit_price * 0.2), 2)
        total_amount = round(quantity * unit_price - discount, 2)
        
        transaction = {
            "transaction_id": cls.generate_transaction_id(),
            "customer_id": customer_id,
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": discount,
            "total_amount": total_amount,
            "transaction_date": cls.generate_datetime(),
            "payment_method": random.choice(cls.PAYMENT_METHODS),
            "status": random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "CANCELLED"]),
            "channel": random.choice(cls.CHANNELS),
        }
        transaction.update(overrides)
        return transaction
    
    @classmethod
    def generate_transactions(
        cls,
        count: int,
        customer_ids: list[str] | None = None,
        product_ids: list[str] | None = None,
        **overrides,
    ) -> list[dict[str, Any]]:
        """Génère plusieurs transactions."""
        return [
            cls.generate_transaction(customer_ids, product_ids, **overrides)
            for _ in range(count)
        ]
    
    @classmethod
    def generate_complete_dataset(
        cls,
        num_customers: int = 100,
        num_products: int = 50,
        num_transactions: int = 500,
    ) -> dict[str, list[dict[str, Any]]]:
        """Génère un dataset complet et cohérent."""
        customers = cls.generate_customers(num_customers)
        products = cls.generate_products(num_products)
        
        customer_ids = [c["customer_id"] for c in customers]
        product_ids = [p["product_id"] for p in products]
        
        transactions = cls.generate_transactions(
            num_transactions,
            customer_ids=customer_ids,
            product_ids=product_ids,
        )
        
        return {
            "customers": customers,
            "products": products,
            "transactions": transactions,
        }


# =============================================================================
# DONNÉES POUR TESTS SPÉCIFIQUES
# =============================================================================

# Données pour tests de transformation
TRANSFORM_TEST_DATA = {
    "simple_aggregation": [
        {"category": "A", "value": 100},
        {"category": "A", "value": 200},
        {"category": "B", "value": 150},
        {"category": "B", "value": 250},
        {"category": "C", "value": 300},
    ],
    "join_left": [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"},
    ],
    "join_right": [
        {"id": 1, "value": 100},
        {"id": 2, "value": 200},
        {"id": 4, "value": 400},
    ],
    "deduplication": [
        {"id": 1, "name": "Alice", "updated_at": "2023-01-01"},
        {"id": 1, "name": "Alice Updated", "updated_at": "2023-06-01"},
        {"id": 2, "name": "Bob", "updated_at": "2023-02-01"},
        {"id": 2, "name": "Bob Updated", "updated_at": "2023-03-01"},
    ],
}

# Données pour tests de qualité
QUALITY_TEST_DATA = {
    "completeness": [
        {"id": 1, "required_field": "value", "optional_field": "opt"},
        {"id": 2, "required_field": "value", "optional_field": None},
        {"id": 3, "required_field": None, "optional_field": "opt"},
        {"id": 4, "required_field": None, "optional_field": None},
    ],
    "uniqueness": [
        {"id": 1, "email": "a@test.com"},
        {"id": 2, "email": "b@test.com"},
        {"id": 3, "email": "a@test.com"},  # Doublon email
        {"id": 1, "email": "c@test.com"},  # Doublon id
    ],
}

# Données CSV brutes pour tests de lecture
RAW_CSV_DATA = """customer_id,email,first_name,last_name,registration_date,is_active
CUST001,john@example.com,John,Doe,2023-01-15,true
CUST002,jane@example.com,Jane,Smith,2023-02-20,true
CUST003,bob@example.com,Bob,Wilson,2023-03-10,false
"""

RAW_CSV_DATA_WITH_ERRORS = """customer_id,email,first_name,last_name,registration_date,is_active
CUST001,john@example.com,John,Doe,2023-01-15,true
CUST002,invalid-email,Jane,Smith,2023-02-20,true
CUST003,bob@example.com,Bob,Wilson,invalid-date,false
,missing@example.com,Missing,ID,2023-04-01,true
"""

# Données JSON brutes pour tests de lecture
RAW_JSON_DATA = [
    {"customer_id": "CUST001", "email": "john@example.com", "first_name": "John"},
    {"customer_id": "CUST002", "email": "jane@example.com", "first_name": "Jane"},
    {"customer_id": "CUST003", "email": "bob@example.com", "first_name": "Bob"},
]


# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

def get_sample_customers(include_invalid: bool = False) -> list[dict[str, Any]]:
    """Retourne les données clients de test."""
    data = SAMPLE_CUSTOMERS_VALID.copy()
    if include_invalid:
        data.extend(SAMPLE_CUSTOMERS_INVALID)
    return data


def get_sample_products(include_invalid: bool = False) -> list[dict[str, Any]]:
    """Retourne les données produits de test."""
    data = SAMPLE_PRODUCTS_VALID.copy()
    if include_invalid:
        data.extend(SAMPLE_PRODUCTS_INVALID)
    return data


def get_sample_transactions(include_invalid: bool = False) -> list[dict[str, Any]]:
    """Retourne les données transactions de test."""
    data = SAMPLE_TRANSACTIONS_VALID.copy()
    if include_invalid:
        data.extend(SAMPLE_TRANSACTIONS_INVALID)
    return data


def get_sample_orders() -> tuple[list[dict], list[dict]]:
    """Retourne les commandes et leurs items."""
    return SAMPLE_ORDERS_VALID.copy(), SAMPLE_ORDER_ITEMS.copy()


def create_test_dataset_small() -> dict[str, list[dict[str, Any]]]:
    """Crée un petit dataset de test (5 clients, 3 produits, 10 transactions)."""
    return {
        "customers": SAMPLE_CUSTOMERS_VALID[:5],
        "products": SAMPLE_PRODUCTS_VALID[:3],
        "transactions": SAMPLE_TRANSACTIONS_VALID[:10],
    }


def create_test_dataset_medium() -> dict[str, list[dict[str, Any]]]:
    """Crée un dataset moyen (50 clients, 20 produits, 200 transactions)."""
    return DataGenerator.generate_complete_dataset(
        num_customers=50,
        num_products=20,
        num_transactions=200,
    )


def create_test_dataset_large() -> dict[str, list[dict[str, Any]]]:
    """Crée un grand dataset (500 clients, 100 produits, 5000 transactions)."""
    return DataGenerator.generate_complete_dataset(
        num_customers=500,
        num_products=100,
        num_transactions=5000,
    )

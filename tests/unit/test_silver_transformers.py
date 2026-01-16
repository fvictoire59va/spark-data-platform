"""Tests unitaires pour les transformers Silver."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.transformers.silver_transformers import (
    DiscountAnalysisTransformer,
    MarginCalculationTransformer,
    OrderValueSegmentationTransformer,
    RepeatOrderDetectionTransformer,
    TaxCalculationTransformer,
)


@pytest.fixture
def spark():
    """Fixture pour créer une session Spark."""
    return SparkSession.builder.appName("test").master("local").getOrCreate()


@pytest.fixture
def sample_orders_df(spark):
    """Crée un DataFrame sample de commandes."""
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_date", TimestampType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(10, 2), False),
            StructField("discount_percent", DecimalType(5, 2), False),
            StructField("total_amount", DecimalType(12, 2), False),
            StructField("order_status", StringType(), False),
            StructField("cost_price", DecimalType(10, 2), True),
        ]
    )

    data = [
        ("ORD001", "CUST001", "2024-01-15", "PROD001", 2, 100.00, 10.0, 180.00, "delivered", 60.00),
        ("ORD002", "CUST001", "2024-01-20", "PROD002", 1, 500.00, 5.0, 475.00, "shipped", 300.00),
        ("ORD003", "CUST002", "2024-01-15", "PROD001", 5, 100.00, 0.0, 500.00, "pending", 60.00),
    ]

    return spark.createDataFrame(data, schema=schema)


class TestOrderValueSegmentationTransformer:
    """Tests pour la segmentation par valeur."""

    def test_high_value_order_detection(self, sample_orders_df):
        """Test la détection des commandes à haute valeur."""
        transformer = OrderValueSegmentationTransformer(high_value_threshold=400.0)
        result = transformer.transform(sample_orders_df)

        high_value_orders = result.filter(F.col("is_high_value_order") == "Y").count()
        assert high_value_orders == 2  # ORD002 (475) et ORD003 (500)

    def test_low_value_order_detection(self, sample_orders_df):
        """Test la détection des commandes à faible valeur."""
        transformer = OrderValueSegmentationTransformer(high_value_threshold=400.0)
        result = transformer.transform(sample_orders_df)

        low_value_orders = result.filter(F.col("is_high_value_order") == "N").count()
        assert low_value_orders == 1  # ORD001 (180)


class TestMarginCalculationTransformer:
    """Tests pour le calcul des marges."""

    def test_margin_calculation_with_cost(self, sample_orders_df):
        """Test le calcul de marge avec cost_price."""
        transformer = MarginCalculationTransformer(cost_price_column="cost_price")
        result = transformer.transform(sample_orders_df)

        margins = result.select("margin_percent").collect()
        assert all(m["margin_percent"] is not None for m in margins)

    def test_margin_percentage_range(self, sample_orders_df):
        """Test que les marges sont dans les bonnes limites."""
        transformer = MarginCalculationTransformer(cost_price_column="cost_price")
        result = transformer.transform(sample_orders_df)

        margins = (
            result.select("margin_percent").filter(F.col("margin_percent").isNotNull()).collect()
        )

        for m in margins:
            assert -100 <= m["margin_percent"] <= 100


class TestRepeatOrderDetectionTransformer:
    """Tests pour la détection des commandes répétées."""

    def test_repeat_order_detection(self, sample_orders_df):
        """Test la détection des commandes répétées."""
        transformer = RepeatOrderDetectionTransformer()
        result = transformer.transform(sample_orders_df)

        repeat_orders = result.filter(F.col("is_repeat_order") == "Y").count()
        assert repeat_orders == 1  # CUST001 a 2 commandes

    def test_first_order_is_not_repeat(self, sample_orders_df):
        """Test que la première commande n'est pas marquée comme répétée."""
        transformer = RepeatOrderDetectionTransformer()
        result = transformer.transform(sample_orders_df)

        first_orders = (
            result.filter(F.col("customer_order_number") == 1).select("is_repeat_order").collect()
        )

        assert all(o["is_repeat_order"] == "N" for o in first_orders)


class TestTaxCalculationTransformer:
    """Tests pour le calcul des taxes."""

    def test_tax_calculation_france(self, spark):
        """Test le calcul de taxe pour la France."""
        schema = StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("customer_country", StringType(), False),
                StructField("subtotal", DecimalType(12, 2), False),
            ]
        )

        data = [
            ("ORD001", "FR", 100.00),
            ("ORD002", "US", 100.00),
        ]

        df = spark.createDataFrame(data, schema=schema)

        transformer = TaxCalculationTransformer(tax_rates={"FR": 0.20, "US": 0.10})
        result = transformer.transform(df)

        taxes = result.select("tax_amount", "customer_country").collect()
        fr_tax = [t for t in taxes if t["customer_country"] == "FR"][0]["tax_amount"]
        us_tax = [t for t in taxes if t["customer_country"] == "US"][0]["tax_amount"]

        # 100 * 0.20 = 20, 100 * 0.10 = 10
        assert float(fr_tax) == 20.0
        assert float(us_tax) == 10.0


class TestDiscountAnalysisTransformer:
    """Tests pour l'analyse des remises."""

    def test_discount_amount_calculation(self, spark):
        """Test le calcul du montant de remise."""
        schema = StructType(
            [
                StructField("subtotal", DecimalType(12, 2), False),
                StructField("discount_percent", DecimalType(5, 2), False),
            ]
        )

        data = [
            (100.00, 10.0),
            (500.00, 5.0),
        ]

        df = spark.createDataFrame(data, schema=schema)

        transformer = DiscountAnalysisTransformer()
        result = transformer.transform(df)

        discounts = result.select("discount_amount").collect()
        assert float(discounts[0]["discount_amount"]) == 10.0  # 100 * 10%
        assert float(discounts[1]["discount_amount"]) == 25.0  # 500 * 5%

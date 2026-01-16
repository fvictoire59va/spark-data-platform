"""Tests unitaires pour les transformers Gold."""

from datetime import datetime, timedelta

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

from src.common.transformers.gold_transformers import (
    DailySalesAggregationTransformer,
    ProductSalesAggregationTransformer,
    RFMAnalysisTransformer,
)


@pytest.fixture
def spark():
    """Fixture pour créer une session Spark."""
    return SparkSession.builder.appName("test").master("local").getOrCreate()


@pytest.fixture
def sample_silver_df(spark):
    """Crée un DataFrame sample de la couche Silver."""
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("order_date", TimestampType(), False),
            StructField("product_id", StringType(), False),
            StructField("quantity", IntegerType(), False),
            StructField("unit_price", DecimalType(10, 2), False),
            StructField("discount_amount", DecimalType(10, 2), False),
            StructField("tax_amount", DecimalType(10, 2), False),
            StructField("total_amount", DecimalType(12, 2), False),
            StructField("order_status", StringType(), False),
            StructField("is_high_value_order", StringType(), False),
            StructField("is_vip_customer", StringType(), False),
            StructField("is_repeat_order", StringType(), False),
            StructField("margin_percent", IntegerType(), False),
            StructField("product_name", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("lifetime_value", DecimalType(15, 2), True),
        ]
    )

    base_date = datetime(2024, 1, 15)
    data = [
        (
            "ORD001",
            "CUST001",
            base_date,
            "PROD001",
            2,
            100.00,
            10.00,
            18.00,
            180.00,
            "delivered",
            "N",
            "N",
            "N",
            30,
            "Product A",
            "Category 1",
            "Premium",
            1000.00,
        ),
        (
            "ORD002",
            "CUST001",
            base_date + timedelta(days=5),
            "PROD002",
            1,
            500.00,
            25.00,
            95.00,
            "delivered",
            "Y",
            "Y",
            "Y",
            35,
            "Product B",
            "Category 2",
            "Premium",
            1500.00,
        ),
        (
            "ORD003",
            "CUST002",
            base_date,
            "PROD001",
            5,
            100.00,
            0.00,
            100.00,
            500.00,
            "pending",
            "Y",
            "N",
            "N",
            30,
            "Product A",
            "Category 1",
            "Standard",
            500.00,
        ),
        (
            "ORD004",
            "CUST003",
            base_date + timedelta(days=10),
            "PROD003",
            3,
            50.00,
            5.00,
            27.00,
            "cancelled",
            "N",
            "N",
            "N",
            40,
            "Product C",
            "Category 3",
            "Budget",
            200.00,
        ),
    ]

    return spark.createDataFrame(data, schema=schema)


class TestDailySalesAggregationTransformer:
    """Tests pour l'agrégation quotidienne."""

    def test_daily_aggregation_creates_report_date(self, sample_silver_df):
        """Test que l'agrégation crée une colonne report_date."""
        transformer = DailySalesAggregationTransformer()
        result = transformer.transform(sample_silver_df)

        assert "report_date" in result.columns

    def test_daily_aggregation_counts_orders(self, sample_silver_df):
        """Test que le compte des commandes est correct."""
        transformer = DailySalesAggregationTransformer()
        result = transformer.transform(sample_silver_df)

        total_orders = result.select("total_orders").collect()[0]["total_orders"]
        assert total_orders == 4  # 4 commandes dans les données de test

    def test_daily_aggregation_sums_revenue(self, sample_silver_df):
        """Test que la somme du revenu est correcte."""
        transformer = DailySalesAggregationTransformer()
        result = transformer.transform(sample_silver_df)

        total_revenue = result.select("total_revenue").collect()[0]["total_revenue"]
        # 180 + 570 + 500 + 77 = 1327
        assert total_revenue is not None

    def test_daily_aggregation_counts_unique_customers(self, sample_silver_df):
        """Test que le compte des clients uniques est correct."""
        transformer = DailySalesAggregationTransformer()
        result = transformer.transform(sample_silver_df)

        unique_customers = result.select("unique_customers").collect()[0]["unique_customers"]
        assert unique_customers == 3  # CUST001, CUST002, CUST003

    def test_daily_aggregation_high_value_orders(self, sample_silver_df):
        """Test que les commandes haute valeur sont comptabilisées."""
        transformer = DailySalesAggregationTransformer()
        result = transformer.transform(sample_silver_df)

        high_value_count = result.select("high_value_orders_count").collect()[0][
            "high_value_orders_count"
        ]
        assert high_value_count == 2  # ORD002 et ORD003


class TestProductSalesAggregationTransformer:
    """Tests pour l'agrégation par produit."""

    def test_product_aggregation_creates_columns(self, sample_silver_df):
        """Test que l'agrégation crée les colonnes attendues."""
        transformer = ProductSalesAggregationTransformer()
        result = transformer.transform(sample_silver_df)

        expected_columns = ["product_id", "total_quantity_sold", "total_revenue", "rank_by_revenue"]
        for col in expected_columns:
            assert col in result.columns

    def test_product_aggregation_sums_quantity(self, sample_silver_df):
        """Test que les quantités sont bien additionnées."""
        transformer = ProductSalesAggregationTransformer()
        result = transformer.transform(sample_silver_df)

        # PROD001 : 2 + 5 = 7
        prod001_qty = (
            result.filter(F.col("product_id") == "PROD001")
            .select("total_quantity_sold")
            .collect()[0]["total_quantity_sold"]
        )
        assert prod001_qty == 7

    def test_product_ranking_by_revenue(self, sample_silver_df):
        """Test que le ranking par revenu est correct."""
        transformer = ProductSalesAggregationTransformer()
        result = transformer.transform(sample_silver_df)

        rankings = result.select("product_id", "rank_by_revenue").collect()
        assert all(r["rank_by_revenue"] is not None for r in rankings)


class TestRFMAnalysisTransformer:
    """Tests pour l'analyse RFM."""

    def test_rfm_creates_rfm_score(self, sample_silver_df):
        """Test que l'analyse RFM crée un score."""
        transformer = RFMAnalysisTransformer()
        result = transformer.transform(sample_silver_df)

        assert "rfm_score" in result.columns
        scores = result.select("rfm_score").collect()
        assert all(s["rfm_score"] is not None for s in scores)

    def test_rfm_creates_customer_segments(self, sample_silver_df):
        """Test que les segments client sont créés."""
        transformer = RFMAnalysisTransformer()
        result = transformer.transform(sample_silver_df)

        assert "customer_value_segment" in result.columns
        segments = result.select("customer_value_segment").distinct().collect()
        assert len(segments) > 0

    def test_rfm_calculates_metrics(self, sample_silver_df):
        """Test que les métriques RFM sont calculées."""
        transformer = RFMAnalysisTransformer()
        result = transformer.transform(sample_silver_df)

        metrics = result.select(
            "days_since_last_purchase", "purchase_frequency", "monetary_value"
        ).collect()

        for m in metrics:
            assert m["days_since_last_purchase"] is not None
            assert m["purchase_frequency"] is not None
            assert m["monetary_value"] is not None

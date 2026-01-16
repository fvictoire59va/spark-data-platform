"""Transformers pour les données."""
from src.common.transformers.base_transformer import (
    BaseTransformer,
    TransformationPipeline,
)
from src.common.transformers.cleaning import (
    DropDuplicatesTransformer,
    DropNullsTransformer,
    FillNullsTransformer,
    TrimStringsTransformer,
)
from src.common.transformers.deduplication import DeduplicationTransformer
from src.common.transformers.validation import (
    SchemaEnforcementTransformer,
    TypeCastTransformer,
)
from src.common.transformers.silver_transformers import (
    SilverTransformer,
    OrderEnrichmentTransformer,
    MarginCalculationTransformer,
    OrderValueSegmentationTransformer,
    RepeatOrderDetectionTransformer,
    DeliveryTimeCalculationTransformer,
    TaxCalculationTransformer,
    DiscountAnalysisTransformer,
    PaymentStatusTransformer,
    FulfillmentStatusTransformer,
)
from src.common.transformers.gold_transformers import (
    GoldTransformer,
    DailySalesAggregationTransformer,
    ProductSalesAggregationTransformer,
    CustomerSalesAggregationTransformer,
    MonthlyTrendsTransformer,
    CustomerSegmentAggregationTransformer,
    TopProductsTransformer,
    RFMAnalysisTransformer,
)

__all__ = [
    "BaseTransformer",
    "TransformationPipeline",
    "DropDuplicatesTransformer",
    "DropNullsTransformer",
    "FillNullsTransformer",
    "TrimStringsTransformer",
    "DeduplicationTransformer",
    "SchemaEnforcementTransformer",
    "TypeCastTransformer",
    # Silver transformers
    "SilverTransformer",
    "OrderEnrichmentTransformer",
    "MarginCalculationTransformer",
    "OrderValueSegmentationTransformer",
    "RepeatOrderDetectionTransformer",
    "DeliveryTimeCalculationTransformer",
    "TaxCalculationTransformer",
    "DiscountAnalysisTransformer",
    "PaymentStatusTransformer",
    "FulfillmentStatusTransformer",
    # Gold transformers
    "GoldTransformer",
    "DailySalesAggregationTransformer",
    "ProductSalesAggregationTransformer",
    "CustomerSalesAggregationTransformer",
    "MonthlyTrendsTransformer",
    "CustomerSegmentAggregationTransformer",
    "TopProductsTransformer",
    "RFMAnalysisTransformer",
]

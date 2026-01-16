"""
Configuration des paramètres métier pour les transformers Silver et Gold.

Ce fichier centralise tous les paramètres configurables pour les transformations.
Il peut être chargé depuis un fichier YAML ou une base de données en production.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class SilverTransformerConfig:
    """Configuration des transformers Silver."""

    # OrderValueSegmentationTransformer
    high_value_threshold: float = 500.0  # Seuil commande haute valeur
    medium_value_threshold: float = 250.0  # Seuil commande valeur moyenne

    # TaxCalculationTransformer
    tax_rates: dict[str, float] = field(default_factory=dict)  # Taux de taxe par pays

    # MarginCalculationTransformer
    cost_price_column: str = "cost_price"  # Colonne du coût
    default_margin: float = 30.0  # Marge par défaut si cost_price absent

    # RepeatOrderDetectionTransformer
    repeat_order_window_days: int = 365  # Fenêtre pour considérer répétition

    # DeliveryTimeCalculationTransformer
    max_delivery_days: int = 30  # Jours maxi livraison normal

    def __post_init__(self):
        """Initialise les configurations par défaut."""
        if not self.tax_rates:
            self.tax_rates = {
                "FR": 0.20,
                "DE": 0.19,
                "IT": 0.22,
                "ES": 0.21,
                "GB": 0.20,
                "BE": 0.21,
                "NL": 0.21,
                "AT": 0.20,
                "PL": 0.23,
                "CZ": 0.21,
                "HU": 0.27,
                "RO": 0.19,
                "GR": 0.24,
                "PT": 0.23,
                "US": 0.10,
                "CA": 0.15,
                "MX": 0.16,
                "BR": 0.18,
                "AU": 0.10,
                "JP": 0.10,
                "CN": 0.13,
                "IN": 0.18,
                "SG": 0.07,
                "HK": 0.00,
            }


@dataclass
class GoldTransformerConfig:
    """Configuration des transformers Gold."""

    # TopProductsTransformer
    top_n_products: int = 20  # Nombre de top produits

    # RFMAnalysisTransformer
    recency_days_threshold: int = 90  # Jours pour At Risk

    # CustomerSegmentAggregationTransformer
    vip_segment_criteria: dict[str, float] = field(default_factory=dict)  # Critères VIP

    # Paramètres généraux
    enable_growth_rate: bool = True  # Calculer growth rate
    enable_ranking: bool = True  # Ajouter rankings
    enable_percentiles: bool = True  # Calculer percentiles

    def __post_init__(self):
        """Initialise les configurations par défaut."""
        if not self.vip_segment_criteria:
            self.vip_segment_criteria = {
                "min_lifetime_value": 5000.0,
                "min_orders": 10.0,
                "min_avg_order_value": 300.0,
                "repeat_purchase_rate": 0.5,  # 50%
            }


@dataclass
class PipelineConfig:
    """Configuration globale du pipeline Silver/Gold."""

    silver_config: Optional[SilverTransformerConfig] = None
    gold_config: Optional[GoldTransformerConfig] = None

    # Partition columns pour optimisation
    silver_partition_cols: Optional[list[str]] = None
    gold_partition_cols: Optional[list[str]] = None

    # Z-order columns pour optimisation
    silver_zorder_cols: Optional[list[str]] = None
    gold_zorder_cols: Optional[list[str]] = None

    # Validation stricte
    fail_on_validation_error: bool = True

    # Caching
    enable_caching: bool = True
    cache_strategy: str = "memory_and_disk"  # "memory", "disk", "memory_and_disk"

    def __post_init__(self):
        """Initialise les configurations par défaut."""
        if self.silver_config is None:
            self.silver_config = SilverTransformerConfig()

        if self.gold_config is None:
            self.gold_config = GoldTransformerConfig()

        if self.silver_partition_cols is None:
            self.silver_partition_cols = ["order_date"]

        if self.gold_partition_cols is None:
            self.gold_partition_cols = ["report_date"]

        if self.silver_zorder_cols is None:
            self.silver_zorder_cols = ["customer_id", "order_date", "product_id"]

        if self.gold_zorder_cols is None:
            self.gold_zorder_cols = ["report_date"]


# ============================================================================
# CONFIGURATIONS PRÉDÉFINIES PAR ENVIRONNEMENT
# ============================================================================

# Configuration DEV
DEV_CONFIG = PipelineConfig(
    silver_config=SilverTransformerConfig(
        high_value_threshold=100.0,  # Threshold bas pour tester
        cost_price_column="cost_price",
        default_margin=25.0,
    ),
    gold_config=GoldTransformerConfig(
        top_n_products=10,  # Moins de produits pour tests
        enable_growth_rate=True,
        enable_ranking=True,
    ),
    fail_on_validation_error=True,
    enable_caching=False,  # Désactiver caching pour faciliter debug
)

# Configuration STAGING
STAGING_CONFIG = PipelineConfig(
    silver_config=SilverTransformerConfig(
        high_value_threshold=400.0,
        cost_price_column="cost_price",
        default_margin=30.0,
    ),
    gold_config=GoldTransformerConfig(
        top_n_products=20,
        enable_growth_rate=True,
        enable_ranking=True,
        enable_percentiles=True,
    ),
    fail_on_validation_error=True,
    enable_caching=True,
    cache_strategy="memory_and_disk",
)

# Configuration PROD
PROD_CONFIG = PipelineConfig(
    silver_config=SilverTransformerConfig(
        high_value_threshold=500.0,
        cost_price_column="cost_price",
        default_margin=35.0,
        repeat_order_window_days=365,
        max_delivery_days=30,
    ),
    gold_config=GoldTransformerConfig(
        top_n_products=20,
        recency_days_threshold=90,
        enable_growth_rate=True,
        enable_ranking=True,
        enable_percentiles=True,
    ),
    fail_on_validation_error=True,
    enable_caching=True,
    cache_strategy="memory_and_disk",
    silver_zorder_cols=["customer_id", "order_date", "product_id"],
    gold_zorder_cols=["report_date"],
)


def get_config_for_environment(environment: str) -> PipelineConfig:
    """
    Récupère la configuration pour un environnement donné.

    Args:
        environment: 'dev', 'staging', ou 'prod'

    Returns:
        PipelineConfig pour cet environnement
    """
    configs = {
        "dev": DEV_CONFIG,
        "staging": STAGING_CONFIG,
        "prod": PROD_CONFIG,
    }

    if environment not in configs:
        raise ValueError(
            f"Environment '{environment}' not found. " f"Available: {list(configs.keys())}"
        )

    return configs[environment]


# ============================================================================
# EXEMPLE D'UTILISATION
# ============================================================================

if __name__ == "__main__":
    # Charger la config pour un environnement
    config = get_config_for_environment("prod")

    # Initialiser si null
    if config.silver_config is None:
        config.silver_config = SilverTransformerConfig()
    if config.gold_config is None:
        config.gold_config = GoldTransformerConfig()

    # Accéder aux paramètres
    print(f"High value threshold: {config.silver_config.high_value_threshold}")
    print(f"Tax rates: {config.silver_config.tax_rates}")
    print(f"Top N products: {config.gold_config.top_n_products}")
    print(f"Enable caching: {config.enable_caching}")

    # Modifier dynamiquement si besoin
    config.silver_config.high_value_threshold = 600.0
    print(f"Updated threshold: {config.silver_config.high_value_threshold}")

# Architecture Silver/Gold - Implémentation Complète

## Vue d'ensemble

Cette implémentation fournit une solution complète et robuste pour les couches **Silver** et **Gold** de l'architecture Medallion dans la plateforme Spark Data.

### Architecture Medallion

```
BRONZE → SILVER → GOLD
(Raw)   (Clean)  (Analytics)
```

## 📊 Couche SILVER - Données Enrichies et Validées

### Objectif
Transformer les données brutes de Bronze en données de qualité avec enrichissements métier, validations et standardisations.

### Schémas Silver

#### 1. **ORDERS_SILVER_ENRICHED_SCHEMA**
Commandes enrichies avec toutes les dimensions commerciales:
- Identifiants: `order_id`, `customer_id`, `product_id`
- Données commerciales: `quantity`, `unit_price`, `discount_percent`, `discount_amount`, `subtotal`, `tax_amount`, `total_amount`
- Statuts: `order_status`, `payment_status`, `fulfillment_status`
- Données client enrichies: `customer_name`, `customer_segment`, `customer_country`, `is_vip_customer`
- Données produit: `product_name`, `product_category`, `product_brand`, `product_price`
- Indicateurs: `margin_percent`, `is_high_value_order`, `is_repeat_order`, `days_to_delivery`

#### 2. **CUSTOMERS_SILVER_SCHEMA**
Table maître des clients:
- Identifiant client, nom, contact
- Segmentation: `customer_segment`, `is_vip`
- Indicateurs: `lifetime_value`, `total_orders`, `is_active`
- Dates: `registration_date`, `last_order_date`

#### 3. **PRODUCTS_SILVER_SCHEMA**
Table maître des produits:
- Identifiant produit, nom, catégorie, marque
- Prix: `unit_price`, `cost_price`
- Stock: `stock_quantity`
- Statut: `is_active`

### Transformers Silver

#### **OrderEnrichmentTransformer**
Enrichit les commandes avec données client et produit via jointures.
```python
transformer = OrderEnrichmentTransformer()
enriched_df = transformer.transform(orders_df, customers_df, products_df)
```

#### **MarginCalculationTransformer**
Calcule le pourcentage de marge:
- Formule: `((prix_unitaire - coût) / prix_unitaire) * 100`
- Gère les cas sans coût disponible avec valeur par défaut

```python
transformer = MarginCalculationTransformer(cost_price_column="cost_price")
df_with_margin = transformer.transform(df)
```

#### **OrderValueSegmentationTransformer**
Segmente les commandes par valeur (seuil configurable):
- `is_high_value_order = 'Y'` si `total_amount >= threshold`
- Seuil par défaut: 500.00

```python
transformer = OrderValueSegmentationTransformer(high_value_threshold=500.0)
df_segmented = transformer.transform(df)
```

#### **RepeatOrderDetectionTransformer**
Détecte les clients qui achètent plusieurs fois:
- Utilise une fenêtre pour numéroter les commandes par client
- `is_repeat_order = 'Y'` si `customer_order_number > 1`

```python
transformer = RepeatOrderDetectionTransformer()
df_repeat = transformer.transform(df)
```

#### **TaxCalculationTransformer**
Calcule les taxes par pays/région:
- Taux configurables par pays
- Applique la taxe au sous-total
- Recalcule le montant total avec taxes

```python
transformer = TaxCalculationTransformer(
    tax_rates={"FR": 0.20, "US": 0.10, "DE": 0.19}
)
df_with_tax = transformer.transform(df)
```

#### **DiscountAnalysisTransformer**
Calcule le montant des remises:
- `discount_amount = subtotal * (discount_percent / 100)`

#### **PaymentStatusTransformer** & **FulfillmentStatusTransformer**
Standardisent les statuts de paiement et fulfillment:
- Mappage cohérent des statuts sources
- Valeurs standards: pending, processing, in_transit, delivered, cancelled

### Job Silver: EnrichOrdersSilverJob

Exécute le pipeline complet d'enrichissement:

```python
job = EnrichOrdersSilverJob(environment=Environment.DEV)
result = job.run()
```

**Étapes:**
1. **Extract**: Lecture depuis Bronze
2. **Transform**: Application de tous les enrichissements
3. **Validate**: Vérification de la qualité
4. **Load**: Écriture en Silver avec optimisation

---

## 🏆 Couche GOLD - Agrégations et KPIs Métier

### Objectif
Fournir des agrégations optimisées et des KPIs pour les analyses et rapports BI.

### Schémas Gold

#### 1. **DAILY_SALES_GOLD_SCHEMA**
Agrégation quotidienne des ventes:
- `total_orders`, `total_revenue`, `average_order_value`
- `total_discount_given`, `total_tax`
- `unique_customers`, `total_items_sold`
- `high_value_orders_count`, `cancelled_orders_count`
- `average_margin_percent`, `vip_orders_count`

#### 2. **PRODUCT_SALES_GOLD_SCHEMA**
Ventes agrégées par produit (quotidien):
- `total_quantity_sold`, `total_revenue`, `number_of_transactions`
- `average_unit_price`, `total_discount`, `total_margin`
- `unique_customers`, `average_quantity_per_order`
- `rank_by_revenue` (classement quotidien)

#### 3. **CUSTOMER_SALES_GOLD_SCHEMA**
Ventes agrégées par client (quotidien):
- `total_spend`, `total_orders`, `average_order_value`
- `total_items_purchased`, `unique_products`
- `repeat_purchase_rate`, `days_since_last_purchase`
- `is_high_value_customer`

#### 4. **MONTHLY_TRENDS_GOLD_SCHEMA**
Tendances mensuelles:
- `total_revenue`, `total_orders`, `total_customers`
- `month_over_month_growth` (calcul automatique)
- `top_product_id`, `top_product_revenue` (top produit du mois)
- `return_rate`, `average_order_value`

#### 5. **CUSTOMER_SEGMENT_GOLD_SCHEMA**
Analyse par segment client:
- `total_customers`, `total_revenue`, `total_orders`
- `average_lifetime_value`, `average_order_frequency`
- `repeat_customer_count`, `repeat_customer_rate`
- `segment_revenue_share` (% du revenu total)

#### 6. **TOP_PRODUCTS_GOLD_SCHEMA**
Classement des meilleurs produits (top 20 par défaut):
- `rank`, `total_quantity_sold`, `total_revenue`
- `unique_customers`
- `growth_rate` (vs période précédente)
- `contribution_to_total` (% du revenu)

#### 7. **RFM_ANALYSIS_GOLD_SCHEMA**
Analyse RFM (Recency, Frequency, Monetary):
- **Recency**: Jours depuis dernier achat
- **Frequency**: Nombre d'achats
- **Monetary**: Valeur totale dépensée
- **RFM_SCORE**: Combinaison des quintiles (ex: "543")
- **CUSTOMER_VALUE_SEGMENT**: Champions, Loyal, At Risk, etc.

### Transformers Gold

#### **DailySalesAggregationTransformer**
Agrège les ventes quotidiennes avec tous les KPIs.

```python
transformer = DailySalesAggregationTransformer()
daily_df = transformer.transform(silver_df)
```

**Agrégations:**
- Comptage des commandes, revenu total
- Moyenne du panier
- Clients uniques, articles vendus
- Commandes haute valeur et annulées
- Marge moyenne, commandes VIP

#### **ProductSalesAggregationTransformer**
Agrège par produit avec ranking.

```python
transformer = ProductSalesAggregationTransformer()
product_df = transformer.transform(silver_df)
```

**Includes:**
- Quantité totale, revenu, transactions
- Prix moyen, remises, marge
- Clients uniques, quantité moyenne par commande
- **Ranking par revenu** (fenêtrée par jour)

#### **CustomerSalesAggregationTransformer**
Agrège par client avec segmentation de valeur.

```python
transformer = CustomerSalesAggregationTransformer()
customer_df = transformer.transform(silver_df)
```

**Includes:**
- Dépenses totales, commandes, valeur moyenne
- Articles achetés, produits uniques
- Taux de réachat, jours depuis dernier achat
- **Segmentation high-value** (percentile-based)

#### **MonthlyTrendsTransformer**
Crée des tendances mensuelles avec growth rate.

```python
transformer = MonthlyTrendsTransformer()
trends_df = transformer.transform(silver_df)
```

**Calculs:**
- Statistiques mensuelles agrégées
- **Growth rate mois-sur-mois** (auto-calculé)
- **Top produit du mois** avec revenu
- Taux de retour

#### **CustomerSegmentAggregationTransformer**
Analyse par segment client.

```python
transformer = CustomerSegmentAggregationTransformer()
segment_df = transformer.transform(silver_df)
```

**Metrics:**
- Clients par segment, revenu segment
- Fréquence moyenne, clients répétés
- **Part du revenu par segment** (%)

#### **TopProductsTransformer**
Identifie les top N produits (défaut 20).

```python
transformer = TopProductsTransformer(top_n=20, spark=spark)
top_df = transformer.transform(silver_df)
```

**Features:**
- Ranking par revenu
- **Growth rate vs période précédente**
- **Contribution au revenu total** (%)

#### **RFMAnalysisTransformer**
Crée l'analyse RFM avec segmentation client.

```python
transformer = RFMAnalysisTransformer(spark=spark)
rfm_df = transformer.transform(silver_df)
```

**Segmentation RFM:**
- **Champions**: Recency 4+, Frequency 4+, Monetary 4+
- **Loyal Customers**: Recency 3+, Frequency 3+, Monetary 3+
- **At Risk**: Recency 4+, Frequency ≤2
- **Cannot Lose Them**: Recency ≤2, Frequency 4+
- **Promising**: Recency 4+, Monetary 3+
- **Needs Attention**: Autres

### Jobs Gold

#### **DailySalesAggregationJob**
```python
job = DailySalesAggregationJob(environment=Environment.DEV)
result = job.run()
```

#### **ProductSalesAggregationJob**
```python
job = ProductSalesAggregationJob(environment=Environment.DEV)
result = job.run()
```

#### **CustomerSalesAggregationJob**
```python
job = CustomerSalesAggregationJob(environment=Environment.DEV)
result = job.run()
```

#### **MonthlyTrendsJob**
```python
job = MonthlyTrendsJob(environment=Environment.DEV)
result = job.run()
```

#### **CustomerSegmentAggregationJob**
```python
job = CustomerSegmentAggregationJob(environment=Environment.DEV)
result = job.run()
```

#### **TopProductsJob**
```python
job = TopProductsJob(environment=Environment.DEV, top_n=20)
result = job.run()
```

#### **RFMAnalysisJob**
```python
job = RFMAnalysisJob(environment=Environment.DEV)
result = job.run()
```

---

## 🔄 Pipeline Orchestration (Airflow)

### DAG Recommandé

```
Bronze Layer
    ↓
Ingest Orders (Bronze)
Ingest Customers (Bronze)
Ingest Products (Bronze)
    ↓
Silver Layer
    ↓
Enrich Orders Silver
    ↓
Gold Layer (Parallel)
    ├─ Daily Sales Aggregation
    ├─ Product Sales Aggregation
    ├─ Customer Sales Aggregation
    ├─ Monthly Trends
    ├─ Customer Segment Analysis
    ├─ Top Products
    └─ RFM Analysis
```

### Configuration Airflow

Chaque job peut être exécuté comme une `SparkSubmitOperator`:

```python
enrich_silver = SparkSubmitOperator(
    task_id="enrich_orders_silver",
    application="/path/to/enrich_orders_silver.py",
    conf={"spark.driver.memory": "4g"},
    dag=dag,
)

daily_sales = SparkSubmitOperator(
    task_id="aggregate_sales_daily",
    application="/path/to/aggregate_gold.py",
    dag=dag,
)

# Dépendances
enrich_silver >> daily_sales
```

---

## 📈 Cas d'Usage et KPIs

### KPIs Quotidiens
- Total des ventes (revenue)
- Nombre de commandes
- Panier moyen
- Clients actifs
- Taux de commandes annulées
- Commandes haute valeur

### KPIs Produits
- Produits top 20 par revenu
- Croissance produit (MoM)
- Mix produits par catégorie
- Clients uniques par produit

### KPIs Clients
- Lifetime Value par client
- Taux de réachat
- Segmentation RFM
- Churn risk (At Risk segment)
- Customer Share of Wallet

### Tendances
- Growth rate MoM
- Saisonnalité
- Top produits du mois
- Évolution des segments

---

## ✅ Validations et Qualité

### Validations Silver
- NOT NULL sur identifiants clés
- Unicité des `order_id`
- Ranges: `quantity` (1-10000), `margin_percent` (-100 à 100)
- Statuts dans ensembles prédéfinis

### Validations Gold
- NOT NULL sur dates et métriques
- Row counts positifs
- Revenue et quantities >= 0
- Rankings cohérents

---

## 📁 Structure des Fichiers

```
src/
├── common/transformers/
│   ├── silver_transformers.py      # Transformers métier Silver
│   └── gold_transformers.py        # Transformers d'agrégation Gold
│
├── pipelines/sales/
│   ├── schemas/
│   │   ├── orders_schema.py
│   │   └── silver_gold_schemas.py  # Tous les schémas Silver/Gold
│   │
│   └── jobs/
│       ├── enrich_orders_silver.py # Job Silver principal
│       ├── aggregate_gold.py       # Jobs Gold (7 classes)
│       └── ...
│
tests/
├── unit/
│   ├── test_silver_transformers.py
│   └── test_gold_transformers.py
```

---

## 🚀 Utilisation

### Exécution Locale
```bash
# Silver
python -m src.pipelines.sales.jobs.enrich_orders_silver \
  --environment dev

# Gold - Daily Sales
python -m src.pipelines.sales.jobs.aggregate_gold \
  --environment dev
```

### Exécution Spark Cluster
```bash
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode cluster \
  src/pipelines/sales/jobs/enrich_orders_silver.py \
  --environment prod
```

---

## 📊 Exemple de Sortie

### Daily Sales
```
report_date    | total_orders | total_revenue | avg_order_value
2024-01-15     | 150          | 75,000.00     | 500.00
2024-01-16     | 165          | 82,500.00     | 500.00
```

### Product Sales
```
report_date | product_id | total_qty | total_revenue | rank
2024-01-15  | PROD001    | 250       | 25,000.00     | 1
2024-01-15  | PROD002    | 150       | 22,500.00     | 2
```

### RFM Analysis
```
customer_id | rfm_score | segment          | days_since_purchase | frequency | monetary_value
CUST001     | 555       | Champions        | 2                   | 12        | 15,000.00
CUST002     | 321       | At Risk          | 45                  | 2         | 1,200.00
```

---

## ⚙️ Configuration et Paramètres

### Transformers Configurables
- **OrderValueSegmentationTransformer**: `high_value_threshold` (défaut 500)
- **TaxCalculationTransformer**: `tax_rates` dict par pays
- **MarginCalculationTransformer**: `cost_price_column`
- **TopProductsTransformer**: `top_n` (défaut 20)

### Configuration Spark
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

---

## 🔒 Performance et Optimisation

### Optimisations Appliquées
- **Z-Ordering** sur colonnes clés (customer_id, order_date, product_id)
- **Partitioning** par date pour les agrégations
- **Caching** des DataFrames réutilisés
- **Broadcast joins** pour les dimensions

### Stats Typiques
- Bronze → Silver: ~2-3x expansion (enrichissements)
- Silver → Gold: ~10-20x réduction (agrégations)
- Volume quotidien: 100K-1M commandes traitées

---

## 📝 Maintenance et Monitoring

### Logs
Les jobs produisent des logs détaillés avec:
- Temps d'exécution de chaque transformer
- Compteurs de lignes et erreurs
- Métriques de performance Spark

### Monitoring Recommandé
- Row counts avant/après chaque étape
- Drift détection sur les KPIs
- Anomalies dans les ratios (margin, discount, etc.)
- Freshness des données (lag)

---

## 🎯 Améliorations Futures

1. **Streaming**: Implémentation Silver/Gold en temps réel avec Structured Streaming
2. **ML Features**: Ajout de features engineering pour ML pipelines
3. **Forecasting**: Prédictions de ventes, churn, demand
4. **Data Catalog**: Intégration Atlas/Hudi pour meilleure gouvernance
5. **Alerting**: Règles métier pour détection d'anomalies

# 🛒 Retail Sales Analytics Lakehouse

### End-to-End Retail Data Engineering & Analytics Platform using Databricks

An end-to-end **Retail Sales Analytics Lakehouse** built on **Databricks** to ingest, clean, transform, model, and analyze retail data from multiple sources.

The project implements a **Medallion Architecture (Bronze → Silver → Gold)** using PySpark, SQL, Delta Lake, Lakeflow Declarative Pipelines, and Databricks SQL. The final data is exposed through a semantic metrics layer and an interactive sales analytics dashboard.

---

## 📌 Table of Contents

- [Business Problem](#-business-problem)
- [Project Objectives](#-project-objectives)
- [Architecture](#-architecture)
- [Data Sources](#-data-sources)
- [Implementation](#-implementation)
- [Data Model](#-data-model)
- [Data Quality](#-data-quality)
- [Semantic Layer](#-semantic-layer)
- [Dashboard & Results](#-dashboard--results)
- [Technology Stack](#-technology-stack)
- [Key Learnings](#-key-learnings)
- [Project Structure](#-project-structure)
- [How to Run](#-how-to-run)
- [Resume Highlights](#-resume-highlights)

---

# 🎯 Business Problem

Retail organizations generate data from multiple operational systems such as:

- Customer/Account management systems
- Sales opportunities
- Product catalogs
- Inventory systems
- Transaction systems

When this data exists in separate sources, business teams face challenges such as:

- Data is distributed across multiple systems.
- Raw data contains inconsistent formats and invalid records.
- Customer, product, sales, and transaction information needs to be integrated.
- Business users need a centralized source for sales analysis.
- Revenue and sales performance must be analyzed across different dimensions.
- Manual data preparation makes reporting slower and less scalable.

### Business Requirement

Build a scalable data platform that can:

1. Ingest data from multiple sources.
2. Validate and standardize incoming data.
3. Integrate related business entities.
4. Build analytics-ready fact and dimension tables.
5. Provide reusable business metrics.
6. Enable interactive sales-performance reporting.

---

# 🎯 Project Objectives

The main objectives were to build an end-to-end data pipeline capable of:

- Ingesting transaction data using Databricks Auto Loader.
- Processing Salesforce and PostgreSQL-based datasets.
- Implementing Bronze, Silver, and Gold layers.
- Applying data-quality rules during transformation.
- Standardizing inconsistent business data.
- Building a sales fact table.
- Creating customer, product, inventory, and calendar dimensions.
- Creating reusable semantic business metrics.
- Building a Databricks SQL sales dashboard.

---

# 🏗️ Architecture

![Retail Sales Analytics Lakehouse Architecture](architecture.png)

### High-Level Flow

```text
                 ┌─────────────────────────────┐
                 │       DATA SOURCES          │
                 ├─────────────────────────────┤
                 │ CSV Transactions            │
                 │ Salesforce Account          │
                 │ Salesforce Opportunity      │
                 │ PostgreSQL Product Catalog  │
                 │ PostgreSQL Inventory        │
                 └──────────────┬──────────────┘
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │       BRONZE LAYER          │
                 │       Raw / Ingestion       │
                 ├─────────────────────────────┤
                 │ Transactions                │
                 │ Salesforce Account         │
                 │ Salesforce Opportunity     │
                 │ Product Catalog             │
                 │ Inventory                   │
                 └──────────────┬──────────────┘
                                │
                         PySpark / DLT
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │       SILVER LAYER          │
                 │   Clean / Standardized      │
                 ├─────────────────────────────┤
                 │ transactions                │
                 │ account                     │
                 │ opportunity                 │
                 │ product_catalog             │
                 │ inventory                   │
                 └──────────────┬──────────────┘
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │        GOLD LAYER           │
                 │    Business / Analytics     │
                 ├─────────────────────────────┤
                 │ fact_sales                  │
                 │ dim_customer                │
                 │ dim_product                 │
                 │ fact_inventory               │
                 │ calendar                    │
                 └──────────────┬──────────────┘
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │      SEMANTIC LAYER         │
                 │     Retail Metrics          │
                 ├─────────────────────────────┤
                 │ Revenue                     │
                 │ Transactions                │
                 │ Quantity Sold               │
                 │ Discounts                   │
                 │ Avg Transaction Value       │
                 │ Unique Customers            │
                 └──────────────┬──────────────┘
                                │
                                ▼
                 ┌─────────────────────────────┐
                 │     DATABRICKS SQL          │
                 │         DASHBOARD           │
                 ├─────────────────────────────┤
                 │ Revenue Trends              │
                 │ Category Performance        │
                 │ Brand Performance            │
                 │ Geographic Analysis         │
                 │ Customer Analysis            │
                 │ Sales Channel               │
                 │ Payment Mode                │
                 └─────────────────────────────┘
```

---

# 📥 Data Sources

The project integrates data representing multiple retail/business systems.

### 1. Transaction Data

Transaction data is provided as CSV files.

Important fields include:

- Transaction ID
- Opportunity Name
- Product ID
- Store ID
- Quantity
- Selling Price
- Discount Amount
- Transaction Timestamp
- Payment Mode
- Sales Channel

The transaction CSV is ingested using **Databricks Auto Loader** with schema tracking and checkpointing.

---

### 2. Salesforce Account

Customer/account information includes:

- Customer ID
- Customer Name
- Customer Type
- Billing City
- Billing State
- Billing Country
- Industry
- Annual Revenue
- Number of Employees

The Silver transformation standardizes customer names and handles missing industry values.

---

### 3. Salesforce Opportunity

Opportunity data provides sales-related information such as:

- Opportunity ID
- Account ID
- Opportunity Name
- Stage
- Amount
- Probability
- Close Date
- Lead Source
- Forecast Category
- Owner

The pipeline also derives a `deal_size` classification:

- ENTERPRISE
- MID_MARKET
- SMALL

and applies validation rules for amount, probability, and opportunity stage. 
---

### 4. PostgreSQL Product Catalog

Product data contains:

- Product ID
- Product Name
- Category
- Subcategory
- Brand
- Unit Price
- Supplier
- Launch Date

The pipeline standardizes product IDs, names, categories, brands, and supplier names and creates product segments based on price. 
---

### 5. PostgreSQL Inventory

Inventory data contains:

- Inventory ID
- Product ID
- Store ID
- Stock Quantity
- Reorder Level
- Warehouse Location
- Last Stock Update

The pipeline derives an `inventory_status` of `LOW_STOCK` or `HEALTHY` based on stock quantity versus reorder level.

---

# ⚙️ Implementation

## 1. Bronze Layer — Data Ingestion

The Bronze layer stores raw ingested data.

For transaction data, **Databricks Auto Loader** is used with:

- CloudFiles
- CSV format
- Schema inference
- Schema location
- Checkpoint location
- `availableNow=True`

This allows new files to be detected and processed incrementally while maintaining ingestion state.

---

# 2. Silver Layer — Cleaning & Transformation

The Silver layer converts raw data into clean and standardized datasets.

### Transactions

Transformations include:

- Data type conversion
- Timestamp parsing
- Gross amount calculation
- Column selection
- Data validation

```python
gross_amount = quantity * selling_price
```

The pipeline also validates transaction ID, quantity, selling price, discount amount, product ID, and payment mode. 
---

### Customer / Account

Customer transformations include:

- Standardizing customer names
- Renaming columns
- Handling missing industry
- Maintaining active-record information
- Selecting business-relevant attributes



---

### Product

Product transformations include:

- Uppercasing product IDs
- Title-casing product names
- Standardizing categories
- Handling missing subcategories and brands
- Rounding unit prices
- Creating product segments
- Tracking active records
- Adding processing timestamps



---

### Inventory

Inventory transformations include:

- Data-quality validation
- Product/store validation
- Stock validation
- Low-stock identification
- Inventory status classification



---

# 3. Gold Layer — Business Modeling

The Gold layer contains business-ready datasets.

## Fact Sales

The central `fact_sales` table combines:

```text
Silver Transactions
        +
Silver Opportunities
        ↓
    fact_sales
```

The integration uses a case-insensitive, trimmed join between transaction opportunity names and opportunity names.

The resulting fact table contains:

- Transaction ID
- Opportunity
- Product ID
- Store ID
- Quantity
- Selling Price
- Discount
- Transaction Timestamp
- Transaction Date
- Payment Mode
- Sales Channel
- Opportunity Stage
- Owner
- Customer ID



---

## Dimension Models

The project creates business-oriented dimensions including:

### Customer Dimension

Contains:

- Customer ID
- Customer Name
- Customer Type
- Billing City
- Billing State
- Billing Country
- Industry
- Annual Revenue
- Employee Count

Only non-deleted and active customer records are exposed in the Gold view.

### Product Dimension

Contains:

- Product ID
- Product Name
- Category
- Subcategory
- Brand
- Product Segment
- Unit Price
- Supplier
- Launch Date

Only active product records are exposed.

### Inventory Fact

Provides:

- Inventory ID
- Product ID
- Stock Quantity
- Reorder Level
- Inventory Status
- Warehouse Location
- Last Stock Update



---

# 📅 Calendar Dimension

A dedicated calendar table is generated dynamically between configurable start and end dates.

It provides:

- Date
- Year
- Quarter
- Month
- Month Name
- Week
- Day
- Weekend/Weekday indicators
- Year-Month
- Year-Quarter
- First/Last day of month



This enables consistent time-based analysis across the sales data.

---

# 🛡️ Data Quality

Data quality rules are implemented using **Lakeflow Declarative Pipelines expectations**.

Examples include:

### Transaction Validation

```text
transaction_id IS NOT NULL
quantity > 0
selling_price >= 0
discount_amount >= 0
product_id IS NOT NULL
payment_mode IN (...)
```



### Product Validation

```text
product_id IS NOT NULL
product_name IS NOT NULL
category IS NOT NULL
unit_price > 0
launch_date IS NOT NULL
supplier_name IS NOT NULL
```



### Opportunity Validation

```text
amount >= 0
probability BETWEEN 0 AND 100
stage_name IN (...)
```



This prevents invalid records from flowing into the business layer and improves trust in downstream analytics.

---

# 📊 Semantic Layer

A dedicated `retail_metrics` semantic view was created on top of the Gold sales fact table.

It connects:

```text
fact_sales
   │
   ├── dim_product
   ├── calendar
   └── dim_customer
```



### Business Dimensions

The semantic layer exposes dimensions such as:

- Transaction Date
- Year
- Quarter
- Month
- Product Category
- Product Brand
- Payment Mode
- Sales Channel
- Opportunity Stage
- Customer Type
- Customer
- City
- State
- Country
- Industry



### Business Metrics

The project defines reusable metrics including:

| Metric | Definition |
|---|---|
| Transaction Count | Number of transactions |
| Total Revenue | Sum of transaction amount |
| Total Quantity Sold | Sum of quantity |
| Total Discount | Sum of discount amount |
| Average Transaction Value | Revenue / transaction count |
| Unique Customers | Distinct customer count |



---

# 📈 Dashboard & Results

The final analytics layer is exposed through a **Databricks SQL dashboard**.

The dashboard provides:

### KPI Cards

- Total Revenue
- Transaction Count
- Unique Customers
- Average Transaction Value

The dashboard configuration explicitly defines these four KPI counters. 
### Sales Analysis

The dashboard provides visual analysis for:

- Revenue over time
- Revenue by product category
- Revenue by brand
- Revenue by state
- Revenue by country
- Revenue by customer type
- Revenue by sales channel
- Revenue by payment mode
- Revenue by opportunity stage

For example, revenue-over-time uses transaction date and total revenue, while category and brand analysis compare total revenue across products. 
### Business Outcome

The resulting platform provides a centralized analytics layer where business users can analyze:

- Sales performance over time
- Product and brand performance
- Geographic revenue distribution
- Customer segments
- Sales channels
- Payment methods
- Opportunity stages
- Core revenue and transaction KPIs

> **Note:** No numeric business-performance improvement is claimed here because the supplied project files do not contain a before/after benchmark or measured production KPI.

---

# 🧰 Technology Stack

| Category | Technology |
|---|---|
| Cloud Data Platform | Databricks |
| Lakehouse | Databricks Lakehouse |
| Storage | Delta Lake |
| Processing | Apache Spark / PySpark |
| Ingestion | Databricks Auto Loader |
| Pipeline | Lakeflow Declarative Pipelines |
| Query Language | SQL |
| Data Modeling | Fact & Dimension Modeling |
| Data Quality | Pipeline Expectations |
| Semantic Layer | Databricks Metrics / YAML |
| Visualization | Databricks SQL Dashboard |
| Governance | Unity Catalog |
| Source Systems | CSV, Salesforce, PostgreSQL |

---

# 🧠 Key Learnings

Through this project, I gained practical experience in:

### 1. Medallion Architecture

Learned how to separate data processing into:

```text
Bronze → Silver → Gold
```

to improve data quality, maintainability, and analytical usability.

### 2. Incremental Data Ingestion

Implemented Databricks Auto Loader with schema tracking and checkpointing for file-based ingestion.

### 3. Data Quality Engineering

Learned how to implement validation rules directly within data pipelines using expectations.

### 4. PySpark Data Transformation

Practiced:

- Data cleansing
- Type casting
- String standardization
- Conditional transformations
- Derived columns
- Joins
- Timestamp processing

### 5. Dimensional Modeling

Learned how to transform operational datasets into analytics-friendly fact and dimension structures.

### 6. Data Integration

Integrated transaction data with opportunity information and connected sales data with customer, product, and calendar dimensions.

### 7. Semantic Modeling

Learned how to create reusable business metrics and dimensions rather than duplicating calculations across dashboards.

### 8. Business Intelligence

Learned how engineering pipelines ultimately support business users through dashboards and reusable KPIs.

---

# 📁 Project Structure

```text
retail-sales-analytics-lakehouse-databricks/
│
├── bronze/
│   └── 01_blob_to_bronze.py
│
├── silver/
│   ├── transactions.py
│   ├── account.py
│   ├── opportunity.py
│   ├── product_catalog.py
│   └── inventory.py
│
├── gold/
│   ├── fact_sales.py
│   ├── 02_Gold_Views.sql
│   └── 03_calendar.py
│
├── semantic/
│   └── 04_Metric_View.py
│
├── dashboards/
│   └── Retail Sales Performance Dashboard.lvdash.json
│
└── README.md
```

---

# ▶️ How to Run

## Step 1 — Configure Data Sources

Configure the required source locations/connections for:

- Transaction CSV files
- Salesforce data
- PostgreSQL data

## Step 2 — Configure Databricks

Create/configure:

- Databricks workspace
- Unity Catalog
- Required catalogs/schemas
- Volumes
- Source connections

## Step 3 — Run Bronze Ingestion

Run:

```text
01_blob_to_bronze.py
```

This loads transaction files into the Bronze layer using Auto Loader.

## Step 4 — Run Silver Pipelines

Run the Silver transformations:

```text
transactions.py
account.py
opportunity.py
product_catalog.py
inventory.py
```

## Step 5 — Build Gold Layer

Run:

```text
fact_sales.py
02_Gold_Views.sql
03_calendar.py
```

## Step 6 — Create Semantic Layer

Run:

```text
04_Metric_View.py
```

This creates the reusable retail metrics layer.

## Step 7 — Create Dashboard

Import:

```text
Retail Sales Performance Dashboard.lvdash.json
```

into Databricks SQL and connect it to the `retail_metrics` semantic dataset.

---

# ⭐ Project Highlights

- End-to-end Databricks Lakehouse implementation
- Multi-source retail data integration
- Auto Loader-based file ingestion
- Bronze/Silver/Gold architecture
- PySpark transformation pipelines
- Lakeflow Declarative Pipelines
- Built-in data-quality expectations
- Fact and dimension modeling
- Dynamic calendar dimension
- Semantic business metrics
- Databricks SQL dashboard
- Revenue and sales-performance analytics

---

# 📌 Resume Project Description

### Retail Sales Analytics Lakehouse | Databricks, PySpark, SQL, Delta Lake

- Built an end-to-end **Databricks Lakehouse pipeline** using Bronze-Silver-Gold architecture to integrate retail transactions, Salesforce accounts/opportunities, product catalog, and inventory data.
- Implemented **Auto Loader and Lakeflow Declarative Pipelines** with schema tracking, checkpoints, data-quality expectations, cleansing, standardization, and incremental processing.
- Developed **Gold-layer fact and dimension models**, including `fact_sales`, customer, product, inventory, and calendar models, and created a reusable semantic metrics layer powering a Databricks SQL sales-performance dashboard.

---

# 💡 Interview Explanation

> “I built a Retail Sales Analytics Lakehouse on Databricks to solve the problem of integrating retail data coming from multiple operational sources. I implemented a Bronze-Silver-Gold architecture. In Bronze, I ingested transaction CSV data using Auto Loader and maintained schema and checkpoint information. In Silver, I used PySpark and Lakeflow Declarative Pipelines to clean and standardize transactions, customers, opportunities, products, and inventory while applying data-quality expectations. In Gold, I integrated transactions with opportunities and created a fact-sales model along with customer, product, inventory, and calendar dimensions. Finally, I created a semantic metrics layer with reusable KPIs such as total revenue, transaction count, quantity sold, average transaction value, and unique customers, which I used to build a Databricks SQL dashboard for sales analysis.”

---

## 👨‍💻 Skills Demonstrated

**Databricks • PySpark • SQL • Delta Lake • Auto Loader • Lakeflow Declarative Pipelines • Data Quality • Medallion Architecture • Dimensional Modeling • Semantic Modeling • Databricks SQL • Data Engineering • Business Intelligence**
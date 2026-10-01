# Sales Data Lakehouse

A medallion architecture (bronze → silver → gold) data lakehouse built on Databricks for sales data from CRM and ERP source systems.

## Architecture

```
CSV Sources (UC Volumes)
    │
    ▼
┌─────────────────────────────────────────────┐
│  BRONZE  (raw ingestion)                    │
│  sales_datalakehouse.bronze                  │
├─────────────────────────────────────────────┤
│  SILVER  (cleaned & standardized)            │
│  sales_datalakehouse.silver                  │
├─────────────────────────────────────────────┤
│  GOLD    (denormalized analytics)            │
│  sales_datalakehouse.gold                    │
└─────────────────────────────────────────────┘
```

## Project Structure

```
SalesDatalakeHouse/
├── README.md
├── .gitignore
├── config folder/
│   └── New File 2026-09-26 23:30:04.py   # ERP ingestion config (INGESTION_CONFIG)
└── Sales_Lakehouse/
    ├── Bronze.ipynb                          # Step 1: CRM ingestion + Step 2: ERP ingestion
    ├── Silver.py                             # Data cleaning & standardization (cust, prd, sales)
    └── Gold Layer Denormalized Analytics.ipynb # Denormalized gold tables with ERP enrichment
```

## Catalog & Schemas

| Schema  | Description                              |
|---------|------------------------------------------|
| bronze  | Raw source data ingested from CSV files  |
| silver  | Cleaned, deduplicated, standardized data  |
| gold    | Denormalized analytics-ready tables       |

### Bronze Tables

| Table                        | Source | Description                          |
|------------------------------|--------|--------------------------------------|
| bronze_crm_cust_info          | CRM    | Customer info (ID, name, gender, etc) |
| bronze_crm_prd_info          | CRM    | Product info (ID, name, cost, line)   |
| bronze_crm_sales_details     | CRM    | Sales line items                      |
| bronze_erp_cust_az12         | ERP    | Customer birth date & gender          |
| bronze_erp_loc_a101          | ERP    | Customer country                      |
| bronze_erp_px_cat_g1v2       | ERP    | Product category & subcategory        |

### Silver Tables

| Table                        | Description                                   |
|------------------------------|-----------------------------------------------|
| silver_crm_cust_info         | Cleaned customers (renamed columns, trimmed)   |
| silver_crm_prd_info          | Cleaned products (standardized categories)     |
| silver_crm_sales_details     | Cleaned sales details                          |

### Gold Tables

| Table                        | Description                                   |
|------------------------------|-----------------------------------------------|
| gold_customer_enriched       | Customers + ERP birth_date & country           |
| gold_sales_denormalized      | Products + ERP category/subcategory           |

## Notebooks

### Bronze — Data Ingestion
- **Step 1 (CRM)**: Manually loads `cust_info.csv`, `prd_info.csv`, and `sales_details.csv` from the `source_crm` volume folder.
- **Step 2 (ERP)**: Uses a config-driven loop (`INGESTION_CONFIG`) to load all ERP CSVs from the `source_erp` volume folder.

### Silver — Data Cleaning
Applies to all bronze tables:
1. DQ analysis (NULLs, whitespace, duplicates, date ranges)
2. Trim whitespace from string columns
3. Deduplicate by primary key (keep latest date, fewest NULLs)
4. Handle NULLs (drop NULL PK, fill others with defaults)
5. Standardize categorical codes (M → Married, R → Road, etc.)
6. Rename columns (drop prefixes, fix abbreviations)
7. Write to silver as managed Delta table

### Gold — Denormalized Analytics
- Joins silver CRM tables with bronze ERP tables for enrichment
- `gold_customer_enriched`: adds birth_date (from `bronze_erp_cust_az12`) and country (from `bronze_erp_loc_a101`)
- `gold_sales_denormalized`: adds category/subcategory (from `bronze_erp_px_cat_g1v2`)

## How to Run

1. Ensure the `sales_datalakehouse` catalog and `bronze`, `silver`, `gold` schemas exist
2. Upload source CSVs to the respective UC volume folders (`source_crm`, `source_erp`)
3. Run notebooks in order: **Bronze → Silver → Gold**

## Source Data

CSV files are stored in Unity Catalog volumes:
- CRM sources: `/Volumes/sales_datalakehouse/bronze/source_system/source_crm/`
- ERP sources: `/Volumes/sales_datalakehouse/bronze/source_system/source_erp/`

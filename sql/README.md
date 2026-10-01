# SQL DDL Scripts

This directory contains SQL DDL (Data Definition Language) scripts to recreate the `sales_datalakehouse` catalog structure in any Databricks workspace.

## Execution Order

Run these scripts in order:

1. **`00_init.sql`** - Creates the catalog and schemas (bronze, silver, gold)
2. **`01_bronze_tables.sql`** - Creates all bronze layer tables (CRM + ERP sources)
3. **`02_silver_tables.sql`** - Creates all silver layer tables (cleaned data)
4. **`03_gold_tables.sql`** - Creates all gold layer tables (denormalized analytics)

## Usage in Databricks

### SQL Editor
1. Open Databricks SQL workspace
2. Create a new query
3. Copy and paste the content of each script
4. Run them in order

### Notebook
```python
# Run each script in order
with open('/Workspace/.../sql/00_init.sql') as f:
    spark.sql(f.read())
```

## Notes

- These scripts use `IF NOT EXISTS` so they're safe to run multiple times
- Table schemas are defined but **data is not included** - run the ingestion notebooks to populate
- The actual data transformation logic is in the notebooks (Bronze → Silver → Gold)

## Version Control

These DDL scripts are version-controlled in Git to:
- Track schema changes over time
- Document the catalog structure
- Enable environment recreation (dev/staging/prod)
- Support Infrastructure as Code practices

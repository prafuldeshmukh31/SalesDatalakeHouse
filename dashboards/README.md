# Dashboards

This directory contains Databricks AI/BI dashboard definitions exported as `.lvdash.json` files.

## Available Dashboards

### 1. Sales Analytics Gold Dashboard
**File**: `sales_analytics_gold_dashboard.lvdash.json`

Analytics dashboard built on the gold layer tables with:
- **KPIs**: Total customers, total products, countries, average cost
- **Customer Analytics**: Distribution by country, gender, marital status
- **Product Analytics**: Top 10 products by cost, product line distribution, category breakdown
- **Data Tables**: Customer and product detail tables
- **Filters**: Global filters for Country and Gender
- **Schedule**: Published and scheduled to refresh daily at 8 AM PT

### 2. Gold Layer Data Quality Dashboard
**File**: `gold_layer_data_quality_dashboard.lvdash.json`

Comprehensive data quality monitoring dashboard with 21 widgets covering:
- **Completeness**: NULL counts for key columns (birth_date, country, category, etc.)
- **Correctness**: Invalid/Unknown value counts
- **Freshness**: Data staleness checks
- **DQ Alerts**: 11 threshold-based checks with PASS/WARN/FAIL indicators

## How to Use

### Import via Databricks UI
1. Go to **Dashboards** in Databricks SQL
2. Click **Create** → **Import dashboard**
3. Upload the `.lvdash.json` file
4. The dashboard will be imported with all widgets, queries, and filters

### Programmatic Import (Python SDK)
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Read dashboard JSON
with open('dashboards/sales_analytics_gold_dashboard.lvdash.json', 'r') as f:
    dashboard_json = f.read()

# Create dashboard
w.lakeview.create(
    display_name="Sales Analytics Gold Dashboard",
    serialized_dashboard=dashboard_json
)
```

## Notes

- These are **exported dashboard definitions** - they contain the complete dashboard structure, widgets, queries, and configurations
- After importing, you may need to update data source connections if catalog/schema names differ
- Dashboard permissions are NOT included in the export - configure them separately after import
- Scheduled refreshes must be reconfigured after import

## Version Control

These dashboard definitions are version-controlled to:
- Track changes to dashboard structure and widgets
- Enable dashboard portability across environments (dev/staging/prod)
- Document the analytics layer of the Sales Lakehouse
- Support Infrastructure as Code practices

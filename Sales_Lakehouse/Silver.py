# Databricks notebook source
# DBTITLE 1,Silver Layer - Customer Cleanup
# MAGIC %md
# MAGIC # Silver Layer — Customer Data Cleanup Pipeline
# MAGIC
# MAGIC This notebook performs data quality analysis and cleanup on the `bronze_crm_cust_info` table from the bronze layer, transforming it into a clean, business-ready table in the `sales_datalakehouse.silver` schema.
# MAGIC
# MAGIC **Pipeline steps:**
# MAGIC
# MAGIC 1. **Data quality analysis** — inspect NULLs, whitespace, duplicates, categorical values, and date ranges
# MAGIC 2. **Trim whitespace** from all string columns
# MAGIC 3. **Deduplicate** records by `cst_id`
# MAGIC 4. **Handle NULLs** — drop junk rows, fill missing gender
# MAGIC 5. **Standardize categories** — `M`/`S` → `Married`/`Single`, `M`/`F` → `Male`/`Female`
# MAGIC 6. **Rename columns** — drop `cst_` prefix for business readability
# MAGIC 7. **Write to silver layer**

# COMMAND ----------

# DBTITLE 1,Data Quality Analysis - cust_info
# MAGIC %md
# MAGIC ## Step 3 — Data Quality Analysis: `bronze_crm_cust_info`
# MAGIC
# MAGIC Before moving data to the silver layer, we need to identify data quality issues in the bronze table. This section inspects NULLs, empty strings, trailing/leading whitespace, duplicate IDs, categorical value distributions, and date ranges.

# COMMAND ----------

# DBTITLE 1,Sample Data
# MAGIC %sql
# MAGIC SELECT * FROM sales_datalakehouse.bronze.bronze_crm_cust_info LIMIT 20

# COMMAND ----------

# DBTITLE 1,NULL, Empty & Whitespace Analysis
# MAGIC %sql
# MAGIC -- NULL, empty string, and whitespace analysis
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     -- NULL counts
# MAGIC     SUM(CASE WHEN cst_id IS NULL THEN 1 ELSE 0 END) AS cst_id_nulls,
# MAGIC     SUM(CASE WHEN cst_key IS NULL THEN 1 ELSE 0 END) AS cst_key_nulls,
# MAGIC     SUM(CASE WHEN cst_firstname IS NULL THEN 1 ELSE 0 END) AS cst_firstname_nulls,
# MAGIC     SUM(CASE WHEN cst_lastname IS NULL THEN 1 ELSE 0 END) AS cst_lastname_nulls,
# MAGIC     SUM(CASE WHEN cst_marital_status IS NULL THEN 1 ELSE 0 END) AS cst_marital_status_nulls,
# MAGIC     SUM(CASE WHEN cst_gndr IS NULL THEN 1 ELSE 0 END) AS cst_gndr_nulls,
# MAGIC     SUM(CASE WHEN cst_create_date IS NULL THEN 1 ELSE 0 END) AS cst_create_date_nulls,
# MAGIC
# MAGIC     -- Empty string counts (not NULL but '')
# MAGIC     SUM(CASE WHEN cst_key = '' THEN 1 ELSE 0 END) AS cst_key_empty,
# MAGIC     SUM(CASE WHEN cst_firstname = '' THEN 1 ELSE 0 END) AS cst_firstname_empty,
# MAGIC     SUM(CASE WHEN cst_lastname = '' THEN 1 ELSE 0 END) AS cst_lastname_empty,
# MAGIC     SUM(CASE WHEN cst_marital_status = '' THEN 1 ELSE 0 END) AS cst_marital_status_empty,
# MAGIC     SUM(CASE WHEN cst_gndr = '' THEN 1 ELSE 0 END) AS cst_gndr_empty,
# MAGIC
# MAGIC     -- Leading/trailing whitespace counts
# MAGIC     SUM(CASE WHEN cst_key != trim(cst_key) THEN 1 ELSE 0 END) AS cst_key_has_whitespace,
# MAGIC     SUM(CASE WHEN cst_firstname != trim(cst_firstname) THEN 1 ELSE 0 END) AS cst_firstname_has_whitespace,
# MAGIC     SUM(CASE WHEN cst_lastname != trim(cst_lastname) THEN 1 ELSE 0 END) AS cst_lastname_has_whitespace,
# MAGIC     SUM(CASE WHEN cst_marital_status != trim(cst_marital_status) THEN 1 ELSE 0 END) AS cst_marital_status_has_whitespace,
# MAGIC     SUM(CASE WHEN cst_gndr != trim(cst_gndr) THEN 1 ELSE 0 END) AS cst_gndr_has_whitespace
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_cust_info

# COMMAND ----------

# DBTITLE 1,Duplicate ID Check
# MAGIC %sql
# MAGIC -- Duplicate check on cst_id and cst_key
# MAGIC SELECT 'cst_id duplicates' AS check_name, COUNT(*) AS dup_count
# MAGIC FROM (
# MAGIC     SELECT cst_id, COUNT(*) AS cnt
# MAGIC     FROM sales_datalakehouse.bronze.bronze_crm_cust_info
# MAGIC     WHERE cst_id IS NOT NULL
# MAGIC     GROUP BY cst_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )
# MAGIC UNION ALL
# MAGIC SELECT 'cst_key duplicates' AS check_name, COUNT(*) AS dup_count
# MAGIC FROM (
# MAGIC     SELECT cst_key, COUNT(*) AS cnt
# MAGIC     FROM sales_datalakehouse.bronze.bronze_crm_cust_info
# MAGIC     WHERE cst_key IS NOT NULL
# MAGIC     GROUP BY cst_key
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Categorical Value Distribution
# MAGIC %sql
# MAGIC -- Distinct values in categorical columns
# MAGIC SELECT 'cst_marital_status' AS column_name, cst_marital_status AS value, COUNT(*) AS count
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_cust_info
# MAGIC GROUP BY cst_marital_status
# MAGIC UNION ALL
# MAGIC SELECT 'cst_gndr' AS column_name, cst_gndr AS value, COUNT(*) AS count
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_cust_info
# MAGIC GROUP BY cst_gndr
# MAGIC ORDER BY column_name, count DESC

# COMMAND ----------

# DBTITLE 1,Date Range Analysis
# MAGIC %sql
# MAGIC -- Date range analysis for cst_create_date
# MAGIC SELECT
# MAGIC     MIN(cst_create_date) AS earliest_date,
# MAGIC     MAX(cst_create_date) AS latest_date,
# MAGIC     COUNT(DISTINCT cst_create_date) AS distinct_dates,
# MAGIC     SUM(CASE WHEN cst_create_date > current_date() THEN 1 ELSE 0 END) AS future_dates,
# MAGIC     SUM(CASE WHEN cst_create_date < '1900-01-01' THEN 1 ELSE 0 END) AS pre_1900_dates
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_cust_info

# COMMAND ----------

# DBTITLE 1,Silver Layer Cleanup - cust_info
# MAGIC %md
# MAGIC ## Step 4 — Silver Layer Cleanup: `bronze_crm_cust_info`
# MAGIC
# MAGIC Based on the data quality analysis, the following cleanup steps will be applied one at a time before writing to the silver layer:
# MAGIC
# MAGIC 1. **Trim whitespace** from all string columns
# MAGIC 2. **Deduplicate** records by `cst_id` / `cst_key`
# MAGIC 3. **Handle NULLs** in `cst_id`, names, `cst_gndr`, `cst_marital_status`, and `cst_create_date`
# MAGIC 4. **Standardize categorical values** (`M`/`S` → `Married`/`Single`, `M`/`F` → `Male`/`Female`)
# MAGIC 5. **Write cleaned data** to the silver layer

# COMMAND ----------

# DBTITLE 1,Step 1 - Trim Whitespace
from pyspark.sql.functions import trim, col

# Read the bronze table
df_cust = spark.read.table("sales_datalakehouse.bronze.bronze_crm_cust_info")

print(f"Rows before trimming: {df_cust.count()}")

# Show rows with whitespace issues before trimming
df_whitespace_issues = df_cust.filter(
    (col("cst_firstname") != trim(col("cst_firstname"))) |
    (col("cst_lastname") != trim(col("cst_lastname"))) |
    (col("cst_key") != trim(col("cst_key"))) |
    (col("cst_marital_status") != trim(col("cst_marital_status"))) |
    (col("cst_gndr") != trim(col("cst_gndr")))
)
print(f"Rows with whitespace issues: {df_whitespace_issues.count()}")
display(df_whitespace_issues.select("cst_id", "cst_firstname", "cst_lastname", "cst_marital_status", "cst_gndr"))

# Step 1: Trim all string columns
df_cust_trimmed = df_cust.select(
    col("cst_id"),
    trim(col("cst_key")).alias("cst_key"),
    trim(col("cst_firstname")).alias("cst_firstname"),
    trim(col("cst_lastname")).alias("cst_lastname"),
    trim(col("cst_marital_status")).alias("cst_marital_status"),
    trim(col("cst_gndr")).alias("cst_gndr"),
    col("cst_create_date")
)

# Verify no whitespace issues remain
df_remaining_issues = df_cust_trimmed.filter(
    (col("cst_firstname") != trim(col("cst_firstname"))) |
    (col("cst_lastname") != trim(col("cst_lastname")))
)
print(f"\nRows with whitespace issues after trim: {df_remaining_issues.count()}")
print("Step 1 complete: All string columns trimmed.")

# COMMAND ----------

# DBTITLE 1,Step 2 - Deduplicate Records
# MAGIC %md
# MAGIC ## Step 2 — Inspect & Deduplicate Records by `cst_id`
# MAGIC
# MAGIC **Refined strategy:**
# MAGIC
# MAGIC 1. Group by `cst_id` and find all duplicate groups
# MAGIC 2. Compare every column value across duplicate rows — not just NULLs, but actual value mismatches
# MAGIC 3. Keep the row with the **latest `cst_create_date`** (since there is no `updated_date` column in this table)
# MAGIC 4. If tied on `cst_create_date`, keep the **most complete row** (fewest NULLs)
# MAGIC 5. If still tied, keep the first row arbitrarily

# COMMAND ----------

# DBTITLE 1,Inspect Duplicate Rows
from pyspark.sql.functions import col, count as count_distinct, when, lit
from pyspark.sql.window import Window

# Find duplicate cst_id groups and show all rows side-by-side
df_dupes = (
    df_cust_trimmed
    .where(col("cst_id").isNotNull())
    .withColumn("dupe_count", count_distinct(lit(1)).over(Window.partitionBy("cst_id")))
    .where(col("dupe_count") > 1)
    .orderBy("cst_id", "cst_create_date")
)

print(f"Duplicate rows (including all copies): {df_dupes.count()}")
print(f"Distinct duplicate cst_ids: {df_dupes.select('cst_id').distinct().count()}")
display(df_dupes.drop("dupe_count"))

# COMMAND ----------

# DBTITLE 1,Apply Deduplication
from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window

# Count NULLs per row (completeness score — lower is better)
df_with_null_count = df_cust_trimmed.withColumn(
    "null_count",
    when(col("cst_key").isNull(), 1).otherwise(0) +
    when(col("cst_firstname").isNull(), 1).otherwise(0) +
    when(col("cst_lastname").isNull(), 1).otherwise(0) +
    when(col("cst_marital_status").isNull(), 1).otherwise(0) +
    when(col("cst_gndr").isNull(), 1).otherwise(0) +
    when(col("cst_create_date").isNull(), 1).otherwise(0)
)

# Window: partition by cst_id, order by latest create_date first, then fewest NULLs
dedupe_window = Window.partitionBy("cst_id").orderBy(
    col("cst_create_date").desc_nulls_last(),  # latest date first
    col("null_count").asc()                     # fewest NULLs as tiebreaker
)

df_ranked = df_with_null_count.withColumn("row_num", row_number().over(dedupe_window))

# Keep only the top-ranked row per cst_id (excluding NULL cst_id rows for now)
df_cust_deduped = (
    df_ranked
    .where(col("cst_id").isNotNull())
    .where(col("row_num") == 1)
    .drop("null_count", "row_num")
)

# Also keep the 4 rows with NULL cst_id (to be handled in Step 3)
df_null_cst_id = (
    df_ranked
    .where(col("cst_id").isNull())
    .drop("null_count", "row_num")
)

# Combine deduped + NULL cst_id rows
df_cust_deduped = df_cust_deduped.unionByName(df_null_cst_id)

print(f"Rows before deduplication: {df_cust_trimmed.count()}")
print(f"Rows after deduplication:  {df_cust_deduped.count()}")
print(f"Duplicate rows removed:    {df_cust_trimmed.count() - df_cust_deduped.count()}")

# Verify no duplicates remain (excluding NULL cst_id)
df_verify_dupes = (
    df_cust_deduped
    .where(col("cst_id").isNotNull())
    .groupBy("cst_id")
    .agg(count_distinct(lit(1)).alias("cnt"))
    .where(col("cnt") > 1)
)
print(f"\nRemaining duplicate cst_ids: {df_verify_dupes.count()}")
print("Step 2 complete: Deduplication applied.")

# COMMAND ----------

# DBTITLE 1,Step 3 - Handle NULLs
# MAGIC %md
# MAGIC ## Step 3 — Inspect & Handle NULLs
# MAGIC
# MAGIC **Strategy:**
# MAGIC
# MAGIC 1. **Inspect** all rows with NULLs to understand patterns and correlations
# MAGIC 2. **Drop** rows with NULL `cst_id` — primary key cannot be missing
# MAGIC 3. **Replace** NULL `cst_firstname` and `cst_lastname` with `"Unknown"`
# MAGIC 4. **Replace** NULL `cst_marital_status` and `cst_gndr` with `"Unknown"` (standardized further in Step 4)
# MAGIC 5. **Keep** NULL `cst_create_date` as-is if any remain after dropping NULL `cst_id` rows (cannot fabricate dates)

# COMMAND ----------

# DBTITLE 1,Inspect NULL Rows
from pyspark.sql.functions import col, when, lit, count as count_distinct

# --- 1. Show rows with NULL cst_id ---
df_null_cst_id = df_cust_deduped.where(col("cst_id").isNull())
print(f"=== Rows with NULL cst_id: {df_null_cst_id.count()} ===")
display(df_null_cst_id)

# --- 2. Show rows with NULL cst_firstname ---
df_null_firstname = df_cust_deduped.where(col("cst_firstname").isNull())
print(f"\n=== Rows with NULL cst_firstname: {df_null_firstname.count()} ===")
display(df_null_firstname)

# --- 3. Show rows with NULL cst_lastname ---
df_null_lastname = df_cust_deduped.where(col("cst_lastname").isNull())
print(f"\n=== Rows with NULL cst_lastname: {df_null_lastname.count()} ===")
display(df_null_lastname)

# --- 4. Show rows with NULL cst_marital_status ---
df_null_marital = df_cust_deduped.where(col("cst_marital_status").isNull())
print(f"\n=== Rows with NULL cst_marital_status: {df_null_marital.count()} ===")
display(df_null_marital)

# --- 5. Show rows with NULL cst_create_date ---
df_null_date = df_cust_deduped.where(col("cst_create_date").isNull())
print(f"\n=== Rows with NULL cst_create_date: {df_null_date.count()} ===")
display(df_null_date)

# --- 6. Check if NULL cst_id and NULL cst_create_date overlap ---
df_both_null = df_cust_deduped.where(col("cst_id").isNull() & col("cst_create_date").isNull())
print(f"\n=== Rows with BOTH NULL cst_id AND NULL cst_create_date: {df_both_null.count()} ===")
display(df_both_null)

# --- 7. Summary: NULL counts in deduped data ---
print("\n=== NULL Summary (post-deduplication) ===")
for col_name in ["cst_id", "cst_key", "cst_firstname", "cst_lastname", "cst_marital_status", "cst_gndr", "cst_create_date"]:
    null_count = df_cust_deduped.where(col(col_name).isNull()).count()
    print(f"  {col_name}: {null_count} NULLs")

# COMMAND ----------

# DBTITLE 1,Apply NULL Handling
from pyspark.sql.functions import col, when, lit

print(f"Rows before NULL handling: {df_cust_deduped.count()}")

# Step 3a: Drop rows with NULL cst_id (junk records — all other columns are also NULL)
df_cust_no_nulls = df_cust_deduped.where(col("cst_id").isNotNull())
print(f"Rows after dropping NULL cst_id: {df_cust_no_nulls.count()}")
print(f"Rows removed: {df_cust_deduped.count() - df_cust_no_nulls.count()}")

# Step 3b: Replace NULL cst_gndr with "Unknown"
df_cust_no_nulls = df_cust_no_nulls.withColumn(
    "cst_gndr",
    when(col("cst_gndr").isNull(), lit("Unknown")).otherwise(col("cst_gndr"))
)

# Verify all NULLs are resolved
print("\n=== NULL Summary after Step 3 ===")
for col_name in ["cst_id", "cst_key", "cst_firstname", "cst_lastname", "cst_marital_status", "cst_gndr", "cst_create_date"]:
    null_count = df_cust_no_nulls.where(col(col_name).isNull()).count()
    print(f"  {col_name}: {null_count} NULLs")

print(f"\nFinal row count: {df_cust_no_nulls.count()}")
print("Step 3 complete: NULL handling applied.")

# COMMAND ----------

# DBTITLE 1,Step 4 - Standardize Categories
# MAGIC %md
# MAGIC ## Step 4 — Standardize Categorical Values
# MAGIC
# MAGIC Replace single-letter codes with full words for readability in the silver layer:
# MAGIC
# MAGIC * `cst_marital_status`: `M` → `Married`, `S` → `Single`
# MAGIC * `cst_gndr`: `M` → `Male`, `F` → `Female`, `Unknown` stays as-is

# COMMAND ----------

# DBTITLE 1,Apply Categorical Standardization
from pyspark.sql.functions import col, when, lit

# Standardize cst_marital_status: M → Married, S → Single
df_cust_standardized = df_cust_no_nulls.withColumn(
    "cst_marital_status",
    when(col("cst_marital_status") == "M", lit("Married"))
    .when(col("cst_marital_status") == "S", lit("Single"))
    .otherwise(col("cst_marital_status"))
)

# Standardize cst_gndr: M → Male, F → Female, Unknown stays as-is
df_cust_standardized = df_cust_standardized.withColumn(
    "cst_gndr",
    when(col("cst_gndr") == "M", lit("Male"))
    .when(col("cst_gndr") == "F", lit("Female"))
    .otherwise(col("cst_gndr"))
)

# Show before and after value distributions
print("=== cst_marital_status after standardization ===")
df_cust_standardized.groupBy("cst_marital_status").count().orderBy("count", ascending=False).show()

print("=== cst_gndr after standardization ===")
df_cust_standardized.groupBy("cst_gndr").count().orderBy("count", ascending=False).show()

print(f"Row count: {df_cust_standardized.count()}")
print("Step 4 complete: Categorical values standardized.")

# COMMAND ----------

# DBTITLE 1,Step 5 - Rename Columns
# MAGIC %md
# MAGIC ## Step 5 — Rename Columns for Business Readability
# MAGIC
# MAGIC Drop the `cst_` prefix and fix the `gndr` abbreviation to make column names business-friendly:
# MAGIC
# MAGIC | Old Name | New Name |
# MAGIC | --- | --- |
# MAGIC | `cst_id` | `customer_id` |
# MAGIC | `cst_key` | `customer_key` |
# MAGIC | `cst_firstname` | `first_name` |
# MAGIC | `cst_lastname` | `last_name` |
# MAGIC | `cst_marital_status` | `marital_status` |
# MAGIC | `cst_gndr` | `gender` |
# MAGIC | `cst_create_date` | `created_date` |

# COMMAND ----------

# DBTITLE 1,Apply Column Renaming
# Rename columns: drop cst_ prefix and fix gndr → gender
df_cust_renamed = df_cust_standardized.select(
    col("cst_id").alias("customer_id"),
    col("cst_key").alias("customer_key"),
    col("cst_firstname").alias("first_name"),
    col("cst_lastname").alias("last_name"),
    col("cst_marital_status").alias("marital_status"),
    col("cst_gndr").alias("gender"),
    col("cst_create_date").alias("created_date")
)

print("Renamed columns:")
for field in df_cust_renamed.schema.fields:
    print(f"  {field.name} ({field.dataType.simpleString()})")

print(f"\nRow count: {df_cust_renamed.count()}")
print("Step 5 complete: Columns renamed.")

# COMMAND ----------

# DBTITLE 1,Step 6 - Write to Silver
# MAGIC %md
# MAGIC ## Step 6 — Write Cleaned Data to Silver Layer
# MAGIC
# MAGIC Write the fully cleaned, standardized, and renamed DataFrame to the `sales_datalakehouse.silver` schema as a managed Delta table. This table is ready for downstream analytics and reporting.

# COMMAND ----------

# DBTITLE 1,Write to Silver Layer
# Create silver schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS sales_datalakehouse.silver")

# Write cleaned data to the silver layer
silver_table = "sales_datalakehouse.silver.silver_crm_cust_info"

df_cust_renamed.write.mode("overwrite").saveAsTable(silver_table)

print(f"Saved to silver table: {silver_table}")
print(f"Row count: {spark.read.table(silver_table).count()}")

# Verify the table
print("\n=== Silver table schema ===")
spark.read.table(silver_table).printSchema()

print("\n=== Sample data ===")
display(spark.read.table(silver_table).limit(10))

print("\nStep 6 complete: Data written to silver layer.")

# COMMAND ----------

# DBTITLE 1,Silver Layer - Product Cleanup
# MAGIC %md
# MAGIC    
# MAGIC # Silver Layer — Product Data Cleanup Pipeline
# MAGIC
# MAGIC This notebook performs data quality analysis and cleanup on the `bronze_crm_prd_info` table from the bronze layer, transforming it into a clean, business-ready table in the `sales_datalakehouse.silver` schema.
# MAGIC
# MAGIC **Pipeline steps:**
# MAGIC
# MAGIC 1. **Data quality analysis** — inspect NULLs, whitespace, duplicates, categorical values, and date ranges
# MAGIC 2. **Trim whitespace** from all string columns
# MAGIC 3. **Deduplicate** records by `prd_id`
# MAGIC 4. **Handle NULLs** — drop junk rows, fill missing cost
# MAGIC 5. **Standardize categories** — `R`/`S`/`M`/`T` → `Road`/`Sport`/`Mountain`/`Touring`
# MAGIC 6. **Rename columns** — drop `prd_` prefix for business readability
# MAGIC 7. **Write to silver layer**

# COMMAND ----------

# DBTITLE 1,Data Quality Analysis - prd_info
# MAGIC %md
# MAGIC    
# MAGIC ## Data Quality Analysis: `bronze_crm_prd_info`
# MAGIC
# MAGIC Before moving data to the silver layer, we need to identify data quality issues in the bronze table. This section inspects NULLs, empty strings, trailing/leading whitespace, duplicate IDs, categorical value distributions, and date ranges.

# COMMAND ----------

# DBTITLE 1,Sample Data - prd_info
# MAGIC %sql
# MAGIC --name prd_sample
# MAGIC SELECT * FROM sales_datalakehouse.bronze.bronze_crm_prd_info LIMIT 20

# COMMAND ----------

# DBTITLE 1,NULL, Empty & Whitespace Analysis - prd_info
# MAGIC %sql
# MAGIC --name prd_null_analysis
# MAGIC -- NULL, empty string, and whitespace analysis
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     -- NULL counts
# MAGIC     SUM(CASE WHEN prd_id IS NULL THEN 1 ELSE 0 END) AS prd_id_nulls,
# MAGIC     SUM(CASE WHEN prd_key IS NULL THEN 1 ELSE 0 END) AS prd_key_nulls,
# MAGIC     SUM(CASE WHEN prd_nm IS NULL THEN 1 ELSE 0 END) AS prd_nm_nulls,
# MAGIC     SUM(CASE WHEN prd_cost IS NULL THEN 1 ELSE 0 END) AS prd_cost_nulls,
# MAGIC     SUM(CASE WHEN prd_line IS NULL THEN 1 ELSE 0 END) AS prd_line_nulls,
# MAGIC     SUM(CASE WHEN prd_start_dt IS NULL THEN 1 ELSE 0 END) AS prd_start_dt_nulls,
# MAGIC     SUM(CASE WHEN prd_end_dt IS NULL THEN 1 ELSE 0 END) AS prd_end_dt_nulls,
# MAGIC
# MAGIC     -- Empty string counts (not NULL but '')
# MAGIC     SUM(CASE WHEN prd_key = '' THEN 1 ELSE 0 END) AS prd_key_empty,
# MAGIC     SUM(CASE WHEN prd_nm = '' THEN 1 ELSE 0 END) AS prd_nm_empty,
# MAGIC     SUM(CASE WHEN prd_line = '' THEN 1 ELSE 0 END) AS prd_line_empty,
# MAGIC
# MAGIC     -- Leading/trailing whitespace counts
# MAGIC     SUM(CASE WHEN prd_key != trim(prd_key) THEN 1 ELSE 0 END) AS prd_key_has_whitespace,
# MAGIC     SUM(CASE WHEN prd_nm != trim(prd_nm) THEN 1 ELSE 0 END) AS prd_nm_has_whitespace,
# MAGIC     SUM(CASE WHEN prd_line != trim(prd_line) THEN 1 ELSE 0 END) AS prd_line_has_whitespace
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_prd_info

# COMMAND ----------

# DBTITLE 1,Duplicate ID Check - prd_info
# MAGIC %sql
# MAGIC --name prd_dup_check
# MAGIC -- Duplicate check on prd_id and prd_key
# MAGIC SELECT 'prd_id duplicates' AS check_name, COUNT(*) AS dup_count
# MAGIC FROM (
# MAGIC     SELECT prd_id, COUNT(*) AS cnt
# MAGIC     FROM sales_datalakehouse.bronze.bronze_crm_prd_info
# MAGIC     WHERE prd_id IS NOT NULL
# MAGIC     GROUP BY prd_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )
# MAGIC UNION ALL
# MAGIC SELECT 'prd_key duplicates' AS check_name, COUNT(*) AS dup_count
# MAGIC FROM (
# MAGIC     SELECT prd_key, COUNT(*) AS cnt
# MAGIC     FROM sales_datalakehouse.bronze.bronze_crm_prd_info
# MAGIC     WHERE prd_key IS NOT NULL
# MAGIC     GROUP BY prd_key
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Categorical Value Distribution - prd_info
# MAGIC %sql
# MAGIC --name prd_cat_dist
# MAGIC -- Distinct values in categorical columns
# MAGIC SELECT 'prd_line' AS column_name, prd_line AS value, COUNT(*) AS count
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_prd_info
# MAGIC GROUP BY prd_line
# MAGIC ORDER BY count DESC

# COMMAND ----------

# DBTITLE 1,Date Range Analysis - prd_info
# MAGIC %sql
# MAGIC --name prd_date_analysis
# MAGIC -- Date range analysis for prd_start_dt and prd_end_dt
# MAGIC SELECT
# MAGIC     MIN(prd_start_dt) AS earliest_start,
# MAGIC     MAX(prd_start_dt) AS latest_start,
# MAGIC     COUNT(DISTINCT prd_start_dt) AS distinct_start_dates,
# MAGIC     SUM(CASE WHEN prd_start_dt > current_date() THEN 1 ELSE 0 END) AS future_start_dates,
# MAGIC     SUM(CASE WHEN prd_start_dt < '1900-01-01' THEN 1 ELSE 0 END) AS pre_1900_start_dates,
# MAGIC     MIN(prd_end_dt) AS earliest_end,
# MAGIC     MAX(prd_end_dt) AS latest_end,
# MAGIC     COUNT(DISTINCT prd_end_dt) AS distinct_end_dates,
# MAGIC     SUM(CASE WHEN prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt THEN 1 ELSE 0 END) AS end_before_start
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_prd_info

# COMMAND ----------

# DBTITLE 1,Inspect: end_date Before start_date Anomaly
# MAGIC %sql
# MAGIC --name prd_end_before_start
# MAGIC -- Show all rows where prd_end_dt is before prd_start_dt
# MAGIC SELECT
# MAGIC     prd_id,
# MAGIC     prd_key,
# MAGIC     prd_nm,
# MAGIC     prd_cost,
# MAGIC     prd_line,
# MAGIC     prd_start_dt,
# MAGIC     prd_end_dt,
# MAGIC     datediff(prd_start_dt, prd_end_dt) AS days_end_before_start
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_prd_info
# MAGIC WHERE prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt
# MAGIC ORDER BY prd_id

# COMMAND ----------

# DBTITLE 1,Silver Layer Cleanup - prd_info
# MAGIC %md
# MAGIC    
# MAGIC ## Silver Layer Cleanup: `bronze_crm_prd_info`
# MAGIC
# MAGIC Based on the data quality analysis, the following cleanup steps will be applied:
# MAGIC
# MAGIC 1. **Trim whitespace** from all string columns (`prd_key`, `prd_nm`, `prd_line`)
# MAGIC 2. **Deduplicate** records by `prd_id` (keep latest `prd_start_dt`, then most complete row)
# MAGIC 3. **Handle NULLs** — drop NULL `prd_id`, fill NULL `prd_cost` with 0
# MAGIC 4. **Standardize categorical values** (`R` → `Road`, `S` → `Sport`, `M` → `Mountain`)
# MAGIC 5. **Rename columns** for business readability
# MAGIC 6. **Write cleaned data** to the silver layer

# COMMAND ----------

# DBTITLE 1,Step 1 - Trim Whitespace (prd_info)
from pyspark.sql.functions import trim, col

# Read the bronze table
df_prd = spark.read.table("sales_datalakehouse.bronze.bronze_crm_prd_info")

print(f"Rows before trimming: {df_prd.count()}")

# Show rows with whitespace issues before trimming
df_whitespace_issues = df_prd.filter(
    (col("prd_key") != trim(col("prd_key"))) |
    (col("prd_nm") != trim(col("prd_nm"))) |
    (col("prd_line") != trim(col("prd_line")))
)
print(f"Rows with whitespace issues: {df_whitespace_issues.count()}")
display(df_whitespace_issues.select("prd_id", "prd_key", "prd_nm", "prd_line"))

# Step 1: Trim all string columns
df_prd_trimmed = df_prd.select(
    col("prd_id"),
    trim(col("prd_key")).alias("prd_key"),
    trim(col("prd_nm")).alias("prd_nm"),
    col("prd_cost"),
    trim(col("prd_line")).alias("prd_line"),
    col("prd_start_dt"),
    col("prd_end_dt")
)

# Verify no whitespace issues remain
df_remaining_issues = df_prd_trimmed.filter(
    (col("prd_key") != trim(col("prd_key"))) |
    (col("prd_nm") != trim(col("prd_nm"))) |
    (col("prd_line") != trim(col("prd_line")))
)
print(f"\nRows with whitespace issues after trim: {df_remaining_issues.count()}")
print("Step 1 complete: All string columns trimmed.")

# COMMAND ----------

# DBTITLE 1,Inspect Duplicate Rows (prd_info)
from pyspark.sql.functions import col, count as count_distinct, when, lit
from pyspark.sql.window import Window

# Find duplicate prd_id groups and show all rows side-by-side
df_dupes = (
    df_prd_trimmed
    .where(col("prd_id").isNotNull())
    .withColumn("dupe_count", count_distinct(lit(1)).over(Window.partitionBy("prd_id")))
    .where(col("dupe_count") > 1)
    .orderBy("prd_id", "prd_start_dt")
)

print(f"Duplicate rows (including all copies): {df_dupes.count()}")
print(f"Distinct duplicate prd_ids: {df_dupes.select('prd_id').distinct().count()}")
display(df_dupes.drop("dupe_count"))

# COMMAND ----------

# DBTITLE 1,Apply Deduplication (prd_info)
from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window

# Count NULLs per row (completeness score — lower is better)
df_with_null_count = df_prd_trimmed.withColumn(
    "null_count",
    when(col("prd_key").isNull(), 1).otherwise(0) +
    when(col("prd_nm").isNull(), 1).otherwise(0) +
    when(col("prd_cost").isNull(), 1).otherwise(0) +
    when(col("prd_line").isNull(), 1).otherwise(0) +
    when(col("prd_start_dt").isNull(), 1).otherwise(0) +
    when(col("prd_end_dt").isNull(), 1).otherwise(0)
)

# Window: partition by prd_id, order by latest prd_start_dt first, then fewest NULLs
dedupe_window = Window.partitionBy("prd_id").orderBy(
    col("prd_start_dt").desc_nulls_last(),  # latest date first
    col("null_count").asc()                   # fewest NULLs as tiebreaker
)

df_ranked = df_with_null_count.withColumn("row_num", row_number().over(dedupe_window))

# Keep only the top-ranked row per prd_id (excluding NULL prd_id rows for now)
df_prd_deduped = (
    df_ranked
    .where(col("prd_id").isNotNull())
    .where(col("row_num") == 1)
    .drop("null_count", "row_num")
)

# Also keep rows with NULL prd_id (to be handled in Step 3)
df_null_prd_id = (
    df_ranked
    .where(col("prd_id").isNull())
    .drop("null_count", "row_num")
)

# Combine deduped + NULL prd_id rows
df_prd_deduped = df_prd_deduped.unionByName(df_null_prd_id)

print(f"Rows before deduplication: {df_prd_trimmed.count()}")
print(f"Rows after deduplication:  {df_prd_deduped.count()}")
print(f"Duplicate rows removed:    {df_prd_trimmed.count() - df_prd_deduped.count()}")

# Verify no duplicates remain (excluding NULL prd_id)
df_verify_dupes = (
    df_prd_deduped
    .where(col("prd_id").isNotNull())
    .groupBy("prd_id")
    .agg(count_distinct(lit(1)).alias("cnt"))
    .where(col("cnt") > 1)
)
print(f"\nRemaining duplicate prd_ids: {df_verify_dupes.count()}")
print("Step 2 complete: Deduplication applied.")

# COMMAND ----------

# DBTITLE 1,Step 3 - Handle NULLs (prd_info)
# MAGIC %md
# MAGIC    
# MAGIC ## Step 3 — Inspect & Handle NULLs
# MAGIC
# MAGIC **Strategy:**
# MAGIC
# MAGIC 1. **Inspect** all rows with NULLs to understand patterns and correlations
# MAGIC 2. **Drop** rows with NULL `prd_id` — primary key cannot be missing
# MAGIC 3. **Replace** NULL `prd_cost` with `0` (unknown cost defaults to zero)
# MAGIC 4. **Replace** NULL `prd_nm` with `"Unknown"`
# MAGIC 5. **Replace** NULL `prd_line` with `"Unknown"`
# MAGIC 6. **Keep** NULL `prd_end_dt` as-is (NULL means product is still active)

# COMMAND ----------

# DBTITLE 1,Inspect NULL Rows (prd_info)
from pyspark.sql.functions import col, when, lit

# --- 1. Show rows with NULL prd_id ---
df_null_prd_id = df_prd_deduped.where(col("prd_id").isNull())
print(f"=== Rows with NULL prd_id: {df_null_prd_id.count()} ===")
display(df_null_prd_id)

# --- 2. Show rows with NULL prd_nm ---
df_null_nm = df_prd_deduped.where(col("prd_nm").isNull())
print(f"\n=== Rows with NULL prd_nm: {df_null_nm.count()} ===")
display(df_null_nm)

# --- 3. Show rows with NULL prd_cost ---
df_null_cost = df_prd_deduped.where(col("prd_cost").isNull())
print(f"\n=== Rows with NULL prd_cost: {df_null_cost.count()} ===")
display(df_null_cost)

# --- 4. Show rows with NULL prd_line ---
df_null_line = df_prd_deduped.where(col("prd_line").isNull())
print(f"\n=== Rows with NULL prd_line: {df_null_line.count()} ===")
display(df_null_line)

# --- 5. Summary: NULL counts in deduped data ---
print("\n=== NULL Summary (post-deduplication) ===")
for col_name in ["prd_id", "prd_key", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt"]:
    null_count = df_prd_deduped.where(col(col_name).isNull()).count()
    print(f"  {col_name}: {null_count} NULLs")

# COMMAND ----------

# DBTITLE 1,Apply NULL Handling (prd_info)
from pyspark.sql.functions import col, when, lit

print(f"Rows before NULL handling: {df_prd_deduped.count()}")

# Step 3a: Drop rows with NULL prd_id (junk records)
df_prd_no_nulls = df_prd_deduped.where(col("prd_id").isNotNull())
print(f"Rows after dropping NULL prd_id: {df_prd_no_nulls.count()}")
print(f"Rows removed: {df_prd_deduped.count() - df_prd_no_nulls.count()}")

# Step 3b: Replace NULL prd_nm with "Unknown"
df_prd_no_nulls = df_prd_no_nulls.withColumn(
    "prd_nm",
    when(col("prd_nm").isNull(), lit("Unknown")).otherwise(col("prd_nm"))
)

# Step 3c: Replace NULL prd_cost with 0
df_prd_no_nulls = df_prd_no_nulls.withColumn(
    "prd_cost",
    when(col("prd_cost").isNull(), lit(0)).otherwise(col("prd_cost"))
)

# Step 3d: Replace NULL prd_line with "Unknown"
df_prd_no_nulls = df_prd_no_nulls.withColumn(
    "prd_line",
    when(col("prd_line").isNull(), lit("Unknown")).otherwise(col("prd_line"))
)

# Verify all NULLs are resolved (except prd_end_dt which is expected to be NULL for active products)
print("\n=== NULL Summary after Step 3 ===")
for col_name in ["prd_id", "prd_key", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt"]:
    null_count = df_prd_no_nulls.where(col(col_name).isNull()).count()
    print(f"  {col_name}: {null_count} NULLs")

print(f"\nFinal row count: {df_prd_no_nulls.count()}")
print("Step 3 complete: NULL handling applied.")

# COMMAND ----------

# DBTITLE 1,Step 4 - Standardize Categories (prd_info)
# MAGIC %md
# MAGIC    
# MAGIC ## Step 4 — Standardize Categorical Values
# MAGIC
# MAGIC Replace single-letter product line codes with full words for readability in the silver layer:
# MAGIC
# MAGIC * `prd_line`: `R` → `Road`, `S` → `Sport`, `M` → `Mountain`, `T` → `Touring`, `Unknown` stays as-is

# COMMAND ----------

# DBTITLE 1,Apply Categorical Standardization (prd_info)
from pyspark.sql.functions import col, when, lit

# Standardize prd_line: R → Road, S → Sport, M → Mountain, T → Touring, Unknown stays as-is
df_prd_standardized = df_prd_no_nulls.withColumn(
    "prd_line",
    when(col("prd_line") == "R", lit("Road"))
    .when(col("prd_line") == "S", lit("Sport"))
    .when(col("prd_line") == "M", lit("Mountain"))
    .when(col("prd_line") == "T", lit("Touring"))
    .otherwise(col("prd_line"))
)

# Show before and after value distributions
print("=== prd_line after standardization ===")
df_prd_standardized.groupBy("prd_line").count().orderBy("count", ascending=False).show()

print(f"Row count: {df_prd_standardized.count()}")
print("Step 4 complete: Categorical values standardized.")

# COMMAND ----------

# DBTITLE 1,Step 5 - Rename Columns (prd_info)
# MAGIC %md
# MAGIC    
# MAGIC ## Step 5 — Rename Columns for Business Readability
# MAGIC
# MAGIC Drop the `prd_` prefix and fix abbreviations to make column names business-friendly:
# MAGIC
# MAGIC | Old Name | New Name |
# MAGIC | --- | --- |
# MAGIC | `prd_id` | `product_id` |
# MAGIC | `prd_key` | `product_key` |
# MAGIC | `prd_nm` | `product_name` |
# MAGIC | `prd_cost` | `cost` |
# MAGIC | `prd_line` | `product_line` |
# MAGIC | `prd_start_dt` | `start_date` |
# MAGIC | `prd_end_dt` | `end_date` |

# COMMAND ----------

# DBTITLE 1,Apply Column Renaming (prd_info)
# Rename columns: drop prd_ prefix and fix abbreviations
df_prd_renamed = df_prd_standardized.select(
    col("prd_id").alias("product_id"),
    col("prd_key").alias("product_key"),
    col("prd_nm").alias("product_name"),
    col("prd_cost").alias("cost"),
    col("prd_line").alias("product_line"),
    col("prd_start_dt").alias("start_date"),
    col("prd_end_dt").alias("end_date")
)

print("Renamed columns:")
for field in df_prd_renamed.schema.fields:
    print(f"  {field.name} ({field.dataType.simpleString()})")

print(f"\nRow count: {df_prd_renamed.count()}")
print("Step 5 complete: Columns renamed.")

# COMMAND ----------

# DBTITLE 1,Step 6 - Write to Silver (prd_info)
# MAGIC %md
# MAGIC    
# MAGIC ## Step 6 — Write Cleaned Data to Silver Layer
# MAGIC
# MAGIC Write the fully cleaned, standardized, and renamed DataFrame to the `sales_datalakehouse.silver` schema as a managed Delta table. This table is ready for downstream analytics and reporting.

# COMMAND ----------

# DBTITLE 1,Write to Silver Layer (prd_info)
# Create silver schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS sales_datalakehouse.silver")

# Write cleaned data to the silver layer
silver_prd_table = "sales_datalakehouse.silver.silver_crm_prd_info"

df_prd_renamed.write.mode("overwrite").saveAsTable(silver_prd_table)

print(f"Saved to silver table: {silver_prd_table}")
print(f"Row count: {spark.read.table(silver_prd_table).count()}")

# Verify the table
print("\n=== Silver table schema ===")
spark.read.table(silver_prd_table).printSchema()

print("\n=== Sample data ===")
display(spark.read.table(silver_prd_table).limit(10))

print("\nStep 6 complete: Data written to silver layer.")

# COMMAND ----------

# DBTITLE 1,Silver Layer - Sales Details Cleanup
# MAGIC %md
# MAGIC    
# MAGIC # Silver Layer — Sales Details Data Cleanup Pipeline
# MAGIC
# MAGIC This notebook performs data quality analysis and cleanup on the `bronze_crm_sales_details` table from the bronze layer, transforming it into a clean, business-ready table in the `sales_datalakehouse.silver` schema.
# MAGIC
# MAGIC **Pipeline steps:**
# MAGIC
# MAGIC 1. **Data quality analysis** — inspect NULLs, whitespace, duplicates, categorical values, and date ranges
# MAGIC 2. **Trim whitespace** from all string columns
# MAGIC 3. **Deduplicate** records by `prd_id`
# MAGIC 4. **Handle NULLs** — drop junk rows, fill missing cost
# MAGIC 5. **Standardize categories** — `R`/`S`/`M`/`T` → `Road`/`Sport`/`Mountain`/`Touring`
# MAGIC 6. **Rename columns** — drop `prd_` prefix for business readability
# MAGIC 7. **Write to silver layer**

# COMMAND ----------

# DBTITLE 1,Data Quality Analysis - sales_details
# MAGIC %md
# MAGIC    
# MAGIC ## Data Quality Analysis: `bronze_crm_sales_details`
# MAGIC
# MAGIC Before moving data to the silver layer, we need to identify data quality issues in the bronze table. This section inspects NULLs, empty strings, trailing/leading whitespace, duplicate IDs, categorical value distributions, and date ranges.

# COMMAND ----------

# DBTITLE 1,Sample Data - sales_details
# MAGIC %sql
# MAGIC --name sls_sample
# MAGIC SELECT * FROM sales_datalakehouse.bronze.bronze_crm_sales_details LIMIT 20

# COMMAND ----------

# DBTITLE 1,NULL, Empty & Whitespace Analysis - sales_details
# MAGIC %sql
# MAGIC --name sls_null_analysis
# MAGIC -- NULL, empty string, and whitespace analysis
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_rows,
# MAGIC
# MAGIC     -- NULL counts
# MAGIC     SUM(CASE WHEN prd_id IS NULL THEN 1 ELSE 0 END) AS prd_id_nulls,
# MAGIC     SUM(CASE WHEN prd_key IS NULL THEN 1 ELSE 0 END) AS prd_key_nulls,
# MAGIC     SUM(CASE WHEN prd_nm IS NULL THEN 1 ELSE 0 END) AS prd_nm_nulls,
# MAGIC     SUM(CASE WHEN prd_cost IS NULL THEN 1 ELSE 0 END) AS prd_cost_nulls,
# MAGIC     SUM(CASE WHEN prd_line IS NULL THEN 1 ELSE 0 END) AS prd_line_nulls,
# MAGIC     SUM(CASE WHEN prd_start_dt IS NULL THEN 1 ELSE 0 END) AS prd_start_dt_nulls,
# MAGIC     SUM(CASE WHEN prd_end_dt IS NULL THEN 1 ELSE 0 END) AS prd_end_dt_nulls,
# MAGIC
# MAGIC     -- Empty string counts (not NULL but '')
# MAGIC     SUM(CASE WHEN prd_key = '' THEN 1 ELSE 0 END) AS prd_key_empty,
# MAGIC     SUM(CASE WHEN prd_nm = '' THEN 1 ELSE 0 END) AS prd_nm_empty,
# MAGIC     SUM(CASE WHEN prd_line = '' THEN 1 ELSE 0 END) AS prd_line_empty,
# MAGIC
# MAGIC     -- Leading/trailing whitespace counts
# MAGIC     SUM(CASE WHEN prd_key != trim(prd_key) THEN 1 ELSE 0 END) AS prd_key_has_whitespace,
# MAGIC     SUM(CASE WHEN prd_nm != trim(prd_nm) THEN 1 ELSE 0 END) AS prd_nm_has_whitespace,
# MAGIC     SUM(CASE WHEN prd_line != trim(prd_line) THEN 1 ELSE 0 END) AS prd_line_has_whitespace
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_sales_details

# COMMAND ----------

# DBTITLE 1,Duplicate ID Check - sales_details
# MAGIC %sql
# MAGIC --name sls_dup_check
# MAGIC -- Duplicate check on prd_id and prd_key
# MAGIC SELECT 'prd_id duplicates' AS check_name, COUNT(*) AS dup_count
# MAGIC FROM (
# MAGIC     SELECT prd_id, COUNT(*) AS cnt
# MAGIC     FROM sales_datalakehouse.bronze.bronze_crm_sales_details
# MAGIC     WHERE prd_id IS NOT NULL
# MAGIC     GROUP BY prd_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )
# MAGIC UNION ALL
# MAGIC SELECT 'prd_key duplicates' AS check_name, COUNT(*) AS dup_count
# MAGIC FROM (
# MAGIC     SELECT prd_key, COUNT(*) AS cnt
# MAGIC     FROM sales_datalakehouse.bronze.bronze_crm_sales_details
# MAGIC     WHERE prd_key IS NOT NULL
# MAGIC     GROUP BY prd_key
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Categorical Value Distribution - sales_details
# MAGIC %sql
# MAGIC --name sls_cat_dist
# MAGIC -- Distinct values in categorical columns
# MAGIC SELECT 'prd_line' AS column_name, prd_line AS value, COUNT(*) AS count
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_sales_details
# MAGIC GROUP BY prd_line
# MAGIC ORDER BY count DESC

# COMMAND ----------

# DBTITLE 1,Date Range Analysis - sales_details
# MAGIC %sql
# MAGIC --name sls_date_analysis
# MAGIC -- Date range analysis for prd_start_dt and prd_end_dt
# MAGIC SELECT
# MAGIC     MIN(prd_start_dt) AS earliest_start,
# MAGIC     MAX(prd_start_dt) AS latest_start,
# MAGIC     COUNT(DISTINCT prd_start_dt) AS distinct_start_dates,
# MAGIC     SUM(CASE WHEN prd_start_dt > current_date() THEN 1 ELSE 0 END) AS future_start_dates,
# MAGIC     SUM(CASE WHEN prd_start_dt < '1900-01-01' THEN 1 ELSE 0 END) AS pre_1900_start_dates,
# MAGIC     MIN(prd_end_dt) AS earliest_end,
# MAGIC     MAX(prd_end_dt) AS latest_end,
# MAGIC     COUNT(DISTINCT prd_end_dt) AS distinct_end_dates,
# MAGIC     SUM(CASE WHEN prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt THEN 1 ELSE 0 END) AS end_before_start
# MAGIC FROM sales_datalakehouse.bronze.bronze_crm_sales_details

# COMMAND ----------

# DBTITLE 1,Silver Layer Cleanup - sales_details
# MAGIC %md
# MAGIC    
# MAGIC ## Silver Layer Cleanup: `bronze_crm_sales_details`
# MAGIC
# MAGIC Based on the data quality analysis, the following cleanup steps will be applied:
# MAGIC
# MAGIC 1. **Trim whitespace** from all string columns (`prd_key`, `prd_nm`, `prd_line`)
# MAGIC 2. **Deduplicate** records by `prd_id` (keep latest `prd_start_dt`, then most complete row)
# MAGIC 3. **Handle NULLs** — drop NULL `prd_id`, fill NULL `prd_cost` with 0, fill NULL `prd_nm` and `prd_line` with "Unknown"
# MAGIC 4. **Standardize categorical values** (`R` → `Road`, `S` → `Sport`, `M` → `Mountain`, `T` → `Touring`)
# MAGIC 5. **Rename columns** for business readability
# MAGIC 6. **Write cleaned data** to the silver layer

# COMMAND ----------

# DBTITLE 1,Step 1 - Trim Whitespace (sales_details)
from pyspark.sql.functions import trim, col

# Read the bronze table
df_sls = spark.read.table("sales_datalakehouse.bronze.bronze_crm_sales_details")

print(f"Rows before trimming: {df_sls.count()}")

# Show rows with whitespace issues before trimming
df_whitespace_issues = df_sls.filter(
    (col("prd_key") != trim(col("prd_key"))) |
    (col("prd_nm") != trim(col("prd_nm"))) |
    (col("prd_line") != trim(col("prd_line")))
)
print(f"Rows with whitespace issues: {df_whitespace_issues.count()}")
display(df_whitespace_issues.select("prd_id", "prd_key", "prd_nm", "prd_line"))

# Step 1: Trim all string columns
df_sls_trimmed = df_sls.select(
    col("prd_id"),
    trim(col("prd_key")).alias("prd_key"),
    trim(col("prd_nm")).alias("prd_nm"),
    col("prd_cost"),
    trim(col("prd_line")).alias("prd_line"),
    col("prd_start_dt"),
    col("prd_end_dt")
)

# Verify no whitespace issues remain
df_remaining_issues = df_sls_trimmed.filter(
    (col("prd_key") != trim(col("prd_key"))) |
    (col("prd_nm") != trim(col("prd_nm"))) |
    (col("prd_line") != trim(col("prd_line")))
)
print(f"\nRows with whitespace issues after trim: {df_remaining_issues.count()}")
print("Step 1 complete: All string columns trimmed.")

# COMMAND ----------

# DBTITLE 1,Inspect Duplicate Rows (sales_details)
from pyspark.sql.functions import col, count as count_distinct, when, lit
from pyspark.sql.window import Window

# Find duplicate prd_id groups and show all rows side-by-side
df_dupes = (
    df_sls_trimmed
    .where(col("prd_id").isNotNull())
    .withColumn("dupe_count", count_distinct(lit(1)).over(Window.partitionBy("prd_id")))
    .where(col("dupe_count") > 1)
    .orderBy("prd_id", "prd_start_dt")
)

print(f"Duplicate rows (including all copies): {df_dupes.count()}")
print(f"Distinct duplicate prd_ids: {df_dupes.select('prd_id').distinct().count()}")
display(df_dupes.drop("dupe_count"))

# COMMAND ----------

# DBTITLE 1,Apply Deduplication (sales_details)
from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window

# Count NULLs per row (completeness score — lower is better)
df_with_null_count = df_sls_trimmed.withColumn(
    "null_count",
    when(col("prd_key").isNull(), 1).otherwise(0) +
    when(col("prd_nm").isNull(), 1).otherwise(0) +
    when(col("prd_cost").isNull(), 1).otherwise(0) +
    when(col("prd_line").isNull(), 1).otherwise(0) +
    when(col("prd_start_dt").isNull(), 1).otherwise(0) +
    when(col("prd_end_dt").isNull(), 1).otherwise(0)
)

# Window: partition by prd_id, order by latest prd_start_dt first, then fewest NULLs
dedupe_window = Window.partitionBy("prd_id").orderBy(
    col("prd_start_dt").desc_nulls_last(),  # latest date first
    col("null_count").asc()                   # fewest NULLs as tiebreaker
)

df_ranked = df_with_null_count.withColumn("row_num", row_number().over(dedupe_window))

# Keep only the top-ranked row per prd_id (excluding NULL prd_id rows for now)
df_sls_deduped = (
    df_ranked
    .where(col("prd_id").isNotNull())
    .where(col("row_num") == 1)
    .drop("null_count", "row_num")
)

# Also keep rows with NULL prd_id (to be handled in Step 3)
df_null_prd_id = (
    df_ranked
    .where(col("prd_id").isNull())
    .drop("null_count", "row_num")
)

# Combine deduped + NULL prd_id rows
df_sls_deduped = df_sls_deduped.unionByName(df_null_prd_id)

print(f"Rows before deduplication: {df_sls_trimmed.count()}")
print(f"Rows after deduplication:  {df_sls_deduped.count()}")
print(f"Duplicate rows removed:    {df_sls_trimmed.count() - df_sls_deduped.count()}")

# Verify no duplicates remain (excluding NULL prd_id)
df_verify_dupes = (
    df_sls_deduped
    .where(col("prd_id").isNotNull())
    .groupBy("prd_id")
    .agg(count_distinct(lit(1)).alias("cnt"))
    .where(col("cnt") > 1)
)
print(f"\nRemaining duplicate prd_ids: {df_verify_dupes.count()}")
print("Step 2 complete: Deduplication applied.")

# COMMAND ----------

# DBTITLE 1,Step 3 - Handle NULLs (sales_details)
# MAGIC %md
# MAGIC    
# MAGIC ## Step 3 — Inspect & Handle NULLs
# MAGIC
# MAGIC **Strategy:**
# MAGIC
# MAGIC 1. **Inspect** all rows with NULLs to understand patterns and correlations
# MAGIC 2. **Drop** rows with NULL `prd_id` — primary key cannot be missing
# MAGIC 3. **Replace** NULL `prd_cost` with `0` (unknown cost defaults to zero)
# MAGIC 4. **Replace** NULL `prd_nm` with `"Unknown"`
# MAGIC 5. **Replace** NULL `prd_line` with `"Unknown"`
# MAGIC 6. **Keep** NULL `prd_end_dt` as-is (NULL means product is still active)

# COMMAND ----------

# DBTITLE 1,Inspect NULL Rows (sales_details)
from pyspark.sql.functions import col, when, lit

# --- 1. Show rows with NULL prd_id ---
df_null_prd_id = df_sls_deduped.where(col("prd_id").isNull())
print(f"=== Rows with NULL prd_id: {df_null_prd_id.count()} ===")
display(df_null_prd_id)

# --- 2. Show rows with NULL prd_nm ---
df_null_nm = df_sls_deduped.where(col("prd_nm").isNull())
print(f"\n=== Rows with NULL prd_nm: {df_null_nm.count()} ===")
display(df_null_nm)

# --- 3. Show rows with NULL prd_cost ---
df_null_cost = df_sls_deduped.where(col("prd_cost").isNull())
print(f"\n=== Rows with NULL prd_cost: {df_null_cost.count()} ===")
display(df_null_cost)

# --- 4. Show rows with NULL prd_line ---
df_null_line = df_sls_deduped.where(col("prd_line").isNull())
print(f"\n=== Rows with NULL prd_line: {df_null_line.count()} ===")
display(df_null_line)

# --- 5. Summary: NULL counts in deduped data ---
print("\n=== NULL Summary (post-deduplication) ===")
for col_name in ["prd_id", "prd_key", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt"]:
    null_count = df_sls_deduped.where(col(col_name).isNull()).count()
    print(f"  {col_name}: {null_count} NULLs")

# COMMAND ----------

# DBTITLE 1,Apply NULL Handling (sales_details)
from pyspark.sql.functions import col, when, lit

print(f"Rows before NULL handling: {df_sls_deduped.count()}")

# Step 3a: Drop rows with NULL prd_id (junk records)
df_sls_no_nulls = df_sls_deduped.where(col("prd_id").isNotNull())
print(f"Rows after dropping NULL prd_id: {df_sls_no_nulls.count()}")
print(f"Rows removed: {df_sls_deduped.count() - df_sls_no_nulls.count()}")

# Step 3b: Replace NULL prd_nm with "Unknown"
df_sls_no_nulls = df_sls_no_nulls.withColumn(
    "prd_nm",
    when(col("prd_nm").isNull(), lit("Unknown")).otherwise(col("prd_nm"))
)

# Step 3c: Replace NULL prd_cost with 0
df_sls_no_nulls = df_sls_no_nulls.withColumn(
    "prd_cost",
    when(col("prd_cost").isNull(), lit(0)).otherwise(col("prd_cost"))
)

# Step 3d: Replace NULL prd_line with "Unknown"
df_sls_no_nulls = df_sls_no_nulls.withColumn(
    "prd_line",
    when(col("prd_line").isNull(), lit("Unknown")).otherwise(col("prd_line"))
)

# Verify all NULLs are resolved (except prd_end_dt which is expected to be NULL for active products)
print("\n=== NULL Summary after Step 3 ===")
for col_name in ["prd_id", "prd_key", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt"]:
    null_count = df_sls_no_nulls.where(col(col_name).isNull()).count()
    print(f"  {col_name}: {null_count} NULLs")

print(f"\nFinal row count: {df_sls_no_nulls.count()}")
print("Step 3 complete: NULL handling applied.")

# COMMAND ----------

# DBTITLE 1,Step 4 - Standardize Categories (sales_details)
# MAGIC %md
# MAGIC    
# MAGIC ## Step 4 — Standardize Categorical Values
# MAGIC
# MAGIC Replace single-letter product line codes with full words for readability in the silver layer:
# MAGIC
# MAGIC * `prd_line`: `R` → `Road`, `S` → `Sport`, `M` → `Mountain`, `T` → `Touring`, `Unknown` stays as-is

# COMMAND ----------

# DBTITLE 1,Apply Categorical Standardization (sales_details)
from pyspark.sql.functions import col, when, lit

# Standardize prd_line: R → Road, S → Sport, M → Mountain, T → Touring, Unknown stays as-is
df_sls_standardized = df_sls_no_nulls.withColumn(
    "prd_line",
    when(col("prd_line") == "R", lit("Road"))
    .when(col("prd_line") == "S", lit("Sport"))
    .when(col("prd_line") == "M", lit("Mountain"))
    .when(col("prd_line") == "T", lit("Touring"))
    .otherwise(col("prd_line"))
)

# Show before and after value distributions
print("=== prd_line after standardization ===")
df_sls_standardized.groupBy("prd_line").count().orderBy("count", ascending=False).show()

print(f"Row count: {df_sls_standardized.count()}")
print("Step 4 complete: Categorical values standardized.")

# COMMAND ----------

# DBTITLE 1,Step 5 - Rename Columns (sales_details)
# MAGIC %md
# MAGIC    
# MAGIC ## Step 5 — Rename Columns for Business Readability
# MAGIC
# MAGIC Drop the `prd_` prefix and fix abbreviations to make column names business-friendly:
# MAGIC
# MAGIC | Old Name | New Name |
# MAGIC | --- | --- |
# MAGIC | `prd_id` | `product_id` |
# MAGIC | `prd_key` | `product_key` |
# MAGIC | `prd_nm` | `product_name` |
# MAGIC | `prd_cost` | `cost` |
# MAGIC | `prd_line` | `product_line` |
# MAGIC | `prd_start_dt` | `start_date` |
# MAGIC | `prd_end_dt` | `end_date` |

# COMMAND ----------

# DBTITLE 1,Apply Column Renaming (sales_details)
# Rename columns: drop prd_ prefix and fix abbreviations
df_sls_renamed = df_sls_standardized.select(
    col("prd_id").alias("product_id"),
    col("prd_key").alias("product_key"),
    col("prd_nm").alias("product_name"),
    col("prd_cost").alias("cost"),
    col("prd_line").alias("product_line"),
    col("prd_start_dt").alias("start_date"),
    col("prd_end_dt").alias("end_date")
)

print("Renamed columns:")
for field in df_sls_renamed.schema.fields:
    print(f"  {field.name} ({field.dataType.simpleString()})")

print(f"\nRow count: {df_sls_renamed.count()}")
print("Step 5 complete: Columns renamed.")

# COMMAND ----------

# DBTITLE 1,Step 6 - Write to Silver (sales_details)
# MAGIC %md
# MAGIC    
# MAGIC ## Step 6 — Write Cleaned Data to Silver Layer
# MAGIC
# MAGIC Write the fully cleaned, standardized, and renamed DataFrame to the `sales_datalakehouse.silver` schema as a managed Delta table. This table is ready for downstream analytics and reporting.

# COMMAND ----------

# DBTITLE 1,Write to Silver Layer (sales_details)
# Create silver schema if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS sales_datalakehouse.silver")

# Write cleaned data to the silver layer
silver_sls_table = "sales_datalakehouse.silver.silver_crm_sales_details"

df_sls_renamed.write.mode("overwrite").saveAsTable(silver_sls_table)

print(f"Saved to silver table: {silver_sls_table}")
print(f"Row count: {spark.read.table(silver_sls_table).count()}")

# Verify the table
print("\n=== Silver table schema ===")
spark.read.table(silver_sls_table).printSchema()

print("\n=== Sample data ===")
display(spark.read.table(silver_sls_table).limit(10))

print("\nStep 6 complete: Data written to silver layer.")
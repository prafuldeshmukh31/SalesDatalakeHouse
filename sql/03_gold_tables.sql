-- ============================================================================
-- Gold Layer Tables - Denormalized Analytics-Ready Tables
-- ============================================================================
-- Enriched with ERP data, optimized for analytics and reporting
-- ============================================================================

USE CATALOG sales_datalakehouse;
USE SCHEMA gold;

-- Customer enriched with ERP birth date and country
CREATE TABLE IF NOT EXISTS gold_customer_enriched (
  customer_id INT NOT NULL COMMENT 'Unique customer identifier',
  customer_key STRING COMMENT 'Customer key from source system',
  first_name STRING COMMENT 'Customer first name',
  last_name STRING COMMENT 'Customer last name',
  marital_status STRING COMMENT 'Marital status',
  gender STRING NOT NULL COMMENT 'Gender',
  birth_date DATE COMMENT 'Birth date (from ERP)',
  country STRING COMMENT 'Country (from ERP)',
  created_date DATE NOT NULL COMMENT 'Customer creation date'
)
COMMENT 'Customer data enriched with ERP birth date and country information';

-- Sales denormalized with ERP category and subcategory
CREATE TABLE IF NOT EXISTS gold_sales_denormalized (
  product_id INT COMMENT 'Unique product identifier',
  product_key STRING COMMENT 'Product key from source system',
  product_name STRING COMMENT 'Product name',
  cost INT COMMENT 'Product cost',
  product_line STRING COMMENT 'Product line',
  product_start_date DATE COMMENT 'Product availability start date',
  product_end_date DATE COMMENT 'Product discontinuation date',
  category STRING COMMENT 'Product category (from ERP)',
  subcategory STRING COMMENT 'Product subcategory (from ERP)',
  maintenance_required STRING COMMENT 'Maintenance required flag (from ERP)',
  master_product_name STRING COMMENT 'Master product name',
  master_cost INT COMMENT 'Master product cost',
  master_product_line STRING COMMENT 'Master product line'
)
COMMENT 'Sales data enriched with ERP category and subcategory information';

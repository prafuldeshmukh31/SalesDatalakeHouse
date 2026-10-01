-- ============================================================================
-- Silver Layer Tables - Cleaned and Standardized Data
-- ============================================================================
-- Data cleaned, deduplicated, standardized columns, and ready for analytics
-- ============================================================================

USE CATALOG sales_datalakehouse;
USE SCHEMA silver;

-- Cleaned customer information
CREATE TABLE IF NOT EXISTS silver_crm_cust_info (
  customer_id INT COMMENT 'Unique customer identifier',
  customer_key STRING COMMENT 'Customer key from source system',
  first_name STRING COMMENT 'Customer first name (cleaned)',
  last_name STRING COMMENT 'Customer last name (cleaned)',
  marital_status STRING COMMENT 'Marital status (standardized)',
  gender STRING COMMENT 'Gender (standardized)',
  created_date DATE COMMENT 'Customer creation date'
)
COMMENT 'Cleaned and standardized customer information';

-- Cleaned product information
CREATE TABLE IF NOT EXISTS silver_crm_prd_info (
  product_id INT COMMENT 'Unique product identifier',
  product_key STRING COMMENT 'Product key from source system',
  product_name STRING COMMENT 'Product name (cleaned)',
  cost INT COMMENT 'Product cost',
  product_line STRING COMMENT 'Product line (standardized)',
  start_date DATE COMMENT 'Product availability start date',
  end_date DATE COMMENT 'Product discontinuation date (NULL if active)'
)
COMMENT 'Cleaned and standardized product information';

-- Cleaned sales details
CREATE TABLE IF NOT EXISTS silver_crm_sales_details (
  product_id INT,
  product_key STRING,
  product_name STRING,
  cost INT,
  product_line STRING,
  start_date DATE,
  end_date DATE
)
COMMENT 'Cleaned and standardized sales line items';

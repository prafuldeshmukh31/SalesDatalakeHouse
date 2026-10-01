-- ============================================================================
-- Bronze Layer Tables - Raw Data Ingestion
-- ============================================================================
-- CRM and ERP source tables (raw, unprocessed)
-- ============================================================================

USE CATALOG sales_datalakehouse;
USE SCHEMA bronze;

-- ----------------------------------------------------------------------------
-- CRM Tables
-- ----------------------------------------------------------------------------

-- Customer information from CRM
CREATE TABLE IF NOT EXISTS bronze_crm_cust_info (
  cst_id INT COMMENT 'Unique identifier assigned to each customer',
  cst_key STRING COMMENT 'Alphanumeric key for additional customer identification',
  cst_firstname STRING COMMENT 'First name of the customer',
  cst_lastname STRING COMMENT 'Last name of the customer',
  cst_marital_status STRING COMMENT 'Marital status for demographic analysis',
  cst_gndr STRING COMMENT 'Gender of the customer',
  cst_create_date DATE COMMENT 'Date when the customer record was created'
)
COMMENT 'Customer information from CRM source system';

-- Product information from CRM
CREATE TABLE IF NOT EXISTS bronze_crm_prd_info (
  prd_id INT COMMENT 'Unique numeric product identifier',
  prd_key STRING COMMENT 'Natural product key from CRM system',
  prd_nm STRING COMMENT 'Product display name',
  prd_cost INT COMMENT 'Standard product cost in currency units',
  prd_line STRING COMMENT 'Product line or category classification',
  prd_start_dt DATE COMMENT 'Date the product became available',
  prd_end_dt DATE COMMENT 'Date the product was discontinued (NULL if active)'
)
COMMENT 'Product information from CRM source system';

-- Sales details from CRM
CREATE TABLE IF NOT EXISTS bronze_crm_sales_details (
  prd_id INT,
  prd_key STRING,
  prd_nm STRING,
  prd_cost INT,
  prd_line STRING,
  prd_start_dt DATE,
  prd_end_dt DATE
)
COMMENT 'Sales line items from CRM source system';

-- ----------------------------------------------------------------------------
-- ERP Tables
-- ----------------------------------------------------------------------------

-- Customer birth date and gender from ERP
CREATE TABLE IF NOT EXISTS bronze_erp_cust_az12 (
  CID STRING COMMENT 'Customer ID for ERP join',
  BDATE DATE COMMENT 'Customer birth date',
  GEN STRING COMMENT 'Customer gender'
)
COMMENT 'Customer birth date and gender from ERP system';

-- Customer location/country from ERP
CREATE TABLE IF NOT EXISTS bronze_erp_loc_a101 (
  CID STRING COMMENT 'Customer ID for ERP join',
  CNTRY STRING COMMENT 'Customer country'
)
COMMENT 'Customer country from ERP system';

-- Product category and subcategory from ERP
CREATE TABLE IF NOT EXISTS bronze_erp_px_cat_g1v2 (
  ID STRING COMMENT 'Product ID for ERP join',
  CAT STRING COMMENT 'Product category',
  SUBCAT STRING COMMENT 'Product subcategory',
  MAINTENANCE STRING COMMENT 'Maintenance required flag'
)
COMMENT 'Product category and subcategory from ERP system';

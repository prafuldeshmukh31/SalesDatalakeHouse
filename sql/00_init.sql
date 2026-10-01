-- ============================================================================
-- Sales Data Lakehouse - Catalog and Schema Initialization
-- ============================================================================
-- This script creates the catalog and schemas for the Sales Data Lakehouse
-- Run this first before creating any tables
-- ============================================================================

-- Create catalog
CREATE CATALOG IF NOT EXISTS sales_datalakehouse
COMMENT 'Sales data lakehouse with medallion architecture (bronze -> silver -> gold)';

USE CATALOG sales_datalakehouse;

-- Create bronze schema (raw data ingestion)
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Bronze layer: Raw data ingested from CRM and ERP source systems';

-- Create silver schema (cleaned and standardized data)
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Silver layer: Cleaned, deduplicated, and standardized data';

-- Create gold schema (denormalized analytics)
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Gold layer: Denormalized, enriched, analytics-ready tables';

-- Display created schemas
SHOW SCHEMAS;

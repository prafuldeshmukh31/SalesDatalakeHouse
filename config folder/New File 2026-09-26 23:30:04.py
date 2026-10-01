# -----------------------------------------------------------------------------
# Ingestion Configuration — Bronze Layer
# Source : ERP CSV files in UC volume
# Target : sales_datalakehouse.bronze schema (one Delta table per source file)
# Usage  : Loop over INGESTION_CONFIG["sources"] and load each file into its
#          corresponding bronze table using the file path and table name.
# -----------------------------------------------------------------------------

INGESTION_CONFIG = {
    "base_path": "/Volumes/sales_datalakehouse/bronze/source_system/source_erp/",
    "target_catalog": "sales_datalakehouse",
    "target_schema": "bronze",
    "sources": [
        {
            "file_name": "CUST_AZ12.csv",
            "table_name": "bronze_erp_cust_az12",
            "description": "Customer master data from ERP source system.",
        },
        {
            "file_name": "LOC_A101.csv",
            "table_name": "bronze_crm_loc_a101",
            "description": "Location / site master data from ERP source system.",
        },
        {
            "file_name": "PX_CAT_G1V2.csv",
            "table_name": "bronze_crm_px_cat_g1v2",
            "description": "Product category reference data from ERP source system.",
        },
    ],
}
/* 
====================================================================
Stored Procedures to Load Data into Bronze Tables
====================================================================
Script Purpose:
    This section contains BULK INSERT commands to load data from CSV files
    into the bronze tables created earlier in this script.
    Actions Taken:
    - Loads data into bronze.crm_cust_info from cust_info.csv
    - Loads data into bronze.crm_prd_info from prd_info.csv
    - Loads data into bronze.crm_sales_details from sales_details.csv
    - Loads data into bronze.erp_loc_a101 from LOC_A101.csv
    - Loads data into bronze.erp_cust_az12 from CUST_AZ12.csv
    - Loads data into bronze.erp_px_cat_g1v2 from PX_CAT_G1V2.csv
NOTE:
    - Ensure the file paths in the BULK INSERT commands are correct and accessible
    - Adjust FIELDTERMINATOR and other parameters as needed based on the CSV format
====================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_data_into_bronze_tables
AS
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME;
    BEGIN TRY 
        SET @start_time = GETDATE();
        PRINT 'Data Load into Bronze Tables Started at: ' + CONVERT(NVARCHAR, @start_time, 120);

        -- BULK INSERT commands to load data into bronze tables can be added here

        SET @end_time = GETDATE();
        PRINT 'Data Load into Bronze Tables Completed at: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT 'Total Duration (seconds): ' + CONVERT(NVARCHAR, DATEDIFF(SECOND, @start_time, @end_time));
        PRINT 'loading CRM Tables into Broze Tables;'

/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

/* BULK INSERT COMMANDS CAN BE ADDED BELOW TO LOAD DATA INTO THE BRONZE TABLES */

-- =====================================================================
-- BULK INSERT: Load CRM Customer Information into Bronze Layer
-- =====================================================================
-- DESCRIPTION:
--   Loads customer information from a CSV file into the bronze.crm_cust_info
--   table. This operation reads from the source CRM dataset and populates
--   the bronze layer with raw customer data for further transformation.
--
-- SOURCE:
--   File: datasets/source_crm/cust_info.csv
--   Format: CSV (Comma-Separated Values)
--
-- TARGET:
--   Schema: bronze
--   Table: crm_cust_info
--
-- PARAMETERS:
--   FIRSTROW = 2        - Skips the header row (row 1) and starts loading from row 2
--   FIELDTERMINATOR = ',' - Specifies comma as the field delimiter
--   TABLOCK             - Uses table-level lock for improved performance during bulk insert
--
-- NOTES:
--   - Assumes the CSV file has a header row in the first line
--   - Requires appropriate file system permissions and valid file path
--   - TABLOCK minimizes transaction log space and improves insert performance
--   - Part of the data warehouse ETL pipeline (bronze layer ingestion)
-- =====================================================================
    SET @batch_start_time = GETDATE();
    PRINT '  Loading bronze.crm_cust_info started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
    PRINT '>> Truncating Table: bronze.crm_cust_info';
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
        EXEC('CREATE SCHEMA bronze');

    IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
        DROP TABLE bronze.crm_cust_info;

    CREATE TABLE bronze.crm_cust_info (
        cst_id              INT,
        cst_key             NVARCHAR(50),
        cst_firstname       NVARCHAR(50),
        cst_lastname        NVARCHAR(50),
        cst_marital_status  NVARCHAR(50),
        cst_gndr            NVARCHAR(50),
        cst_create_date     DATE
    );
    PRINT '>> Inserting Data Into: bronze.crm_cust_info';

    BULK INSERT bronze.crm_cust_info
    FROM '/var/opt/mssql/data/datasets/source_crm/cust_info.csv'
    WITH(
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @batch_end_time = GETDATE();
        PRINT '  Loading bronze.crm_cust_info completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- =====================================================================
-- BULK INSERT: Load CRM Prd Info into Bronze Layer
-- =====================================================================
    SET @batch_start_time = GETDATE();
    PRINT '  Loading bronze.crm_prd_info started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
    PRINT '>> Truncating Table: bronze.crm_prd_info';
    
    IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
        DROP TABLE bronze.crm_prd_info;

    CREATE TABLE bronze.crm_prd_info (
        prd_id       INT,
        prd_key      NVARCHAR(50),
        prd_nm       NVARCHAR(50),
        prd_cost     INT,
        prd_line     NVARCHAR(50),
        prd_start_dt DATETIME,
        prd_end_dt   DATETIME
    );

    PRINT '>> Inserting Data Into: bronze.crm_prd_info';

    BULK INSERT bronze.crm_prd_info
    FROM '/var/opt/mssql/data/datasets/source_crm/prd_info.csv'
    WITH(
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    )

    SET @batch_end_time = GETDATE();
        PRINT '  Loading bronze.crm_prd_info completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- =====================================================================
-- BULK INSERT: Load CRM Sales Details into Bronze Layer
-- =====================================================================
    SET @batch_start_time = GETDATE();
    PRINT '  Loading bronze.crm_sales_details started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
    PRINT '>> Truncating Table: bronze.crm_sales_details';

    IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
        DROP TABLE bronze.crm_sales_details;

    CREATE TABLE bronze.crm_sales_details (
        sls_ord_num  NVARCHAR(50),
        sls_prd_key  NVARCHAR(50),
        sls_cust_id  INT,
        sls_order_dt INT,
        sls_ship_dt  INT,
        sls_due_dt   INT,
        sls_sales    INT,
        sls_quantity INT,
        sls_price    INT
    );
    PRINT '>> Inserting Data Into: bronze.crm_sales_details';

    BULK INSERT bronze.crm_sales_details
    FROM '/var/opt/mssql/data/datasets/source_crm/sales_details.csv'
    WITH(
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    )

    SET @batch_end_time = GETDATE();
        PRINT '  Loading bronze.crm_sales_details completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- =====================================================================
-- BULK INSERT: Load ERP Location Data into Bronze Layer
-- =====================================================================
    SET @batch_start_time = GETDATE();
    PRINT '  Loading bronze.erp_loc_a101 started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
    PRINT '>> Truncating Table: bronze.erp_loc_a101';

    IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
        DROP TABLE bronze.erp_loc_a101;

    CREATE TABLE bronze.erp_loc_a101 (
        cid    NVARCHAR(50),
        cntry  NVARCHAR(50)
    );

    PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        
    BULK INSERT bronze.erp_loc_a101
    FROM '/var/opt/mssql/data/datasets/source_erp/LOC_A101.csv'
    WITH(
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    )

    SET @batch_end_time = GETDATE();
        PRINT '  Loading bronze.erp_loc_a101 completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- =====================================================================
-- BULK INSERT: Load ERP Customer Data into Bronze Layer
-- =====================================================================
    SET @batch_start_time = GETDATE();
    PRINT '  Loading bronze.erp_cust_az12 started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
    PRINT '>> Truncating Table: bronze.erp_cust_az12';

    IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
        DROP TABLE bronze.erp_cust_az12;

    CREATE TABLE bronze.erp_cust_az12 (
        cid    NVARCHAR(50),
        bdate  DATE,
        gen    NVARCHAR(50)
    );

    PRINT '>> Inserting Data Into: bronze.erp_cust_az12';

    BULK INSERT bronze.erp_cust_az12
    FROM '/var/opt/mssql/data/datasets/source_erp/CUST_AZ12.csv'
    WITH(
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    )

    SET @batch_end_time = GETDATE();
        PRINT '  Loading bronze.erp_cust_az12 completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

-- =====================================================================
-- BULK INSERT: Load ERP Product Category Data into Bronze Layer
-- =====================================================================
    SET @batch_start_time = GETDATE();
    PRINT '  Loading bronze.erp_px_cat_g1v2 started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
    PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';

    IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;

    CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
    );

    BULK INSERT bronze.erp_px_cat_g1v2
    FROM '/var/opt/mssql/data/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH(
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    )
    SET @batch_end_time = GETDATE();
        PRINT '  Loading bronze.erp_px_cat_g1v2 completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

END TRY 
    BEGIN CATCH 
        PRINT '=========================================='
            PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
            PRINT 'Error Message' + ERROR_MESSAGE();
            PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
            PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
            PRINT '=========================================='
	END CATCH
END

-- EXECUTE THE PROCEDURE TO LOAD DATA INTO BRONZE TABLES
EXEC bronze.load_data_into_bronze_tables;
GO
SELECT * FROM bronze.crm_cust_info;
GO
SELECT * FROM bronze.crm_prd_info;
GO
SELECT * FROM bronze.crm_sales_details; 
GO
SELECT * FROM bronze.erp_loc_a101;
GO    
SELECT * FROM bronze.erp_cust_az12;
GO
SELECT * FROM bronze.erp_px_cat_g1v2;
GO
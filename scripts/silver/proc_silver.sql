/* 
====================================================================
Stored Procedures to Load Data into Silver Tables
====================================================================
Script Purpose:
    This section contains stored procedures to load cleaned data
    into the SILVER tables created earlier in this script.
    Actions Taken:
    - Loads data into silver.crm_cust_info from cust_info.csv
    - Loads data into silver.crm_prd_info from prd_info.csv
    - Loads data into silver.crm_sales_details from sales_details.csv
    - Loads data into silver.erp_loc_a101 from LOC_A101.csv
    - Loads data into silver.erp_cust_az12 from CUST_AZ12.csv
    - Loads data into silver.erp_px_cat_g1v2 from PX_CAT_G1V2.csv
NOTE:
    - Ensure the file paths in the INSERT STATEMENTS are correct.
====================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
     DECLARE @start_time DATETIME,@end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME;
    BEGIN TRY 
        SET @start_time = GETDATE();
        PRINT 'Data Load into Silver Tables Started at: ' + CONVERT(NVARCHAR, @start_time, 120);

        -- BULK INSERT commands to load data into silver tables can be added here

        SET @end_time = GETDATE();
        PRINT 'Data Load into Silver Tables Completed at: ' + CONVERT(NVARCHAR, @end_time, 120);
        PRINT 'Total Duration (seconds): ' + CONVERT(NVARCHAR, DATEDIFF(SECOND, @start_time, @end_time));
        PRINT 'loading CRM Tables into Silver Tables;'
/*
========================================================================
 CLEANING DATA FROM BRONZE LAYER TO SILVER LAYER IN CRM_CUST_INFO 
========================================================================
*/
--- LOADING CLEANED DATA INTO SILVER LAYER IN SILVER.CRM_CUST_INFO ---
--- REMOVE DUPLICATES BASED ON MOST RECENT RECORD ---
--- TRIMMING WHITESPACES FROM FIRST AND LAST NAMES ---
--- STANDARDIZING MARITAL STATUS AND GENDER VALUES ---

SET @batch_start_time = GETDATE();
    PRINT '  Loading silver.crm_cust_info started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> Loading cleaned data into silver.crm_cust_info';
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date)  

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE UPPER(TRIM(cst_marital_status))
    WHEN 'M' THEN 'Married'
    WHEN 'S' THEN 'Single'
    ELSE 'Unknown'
END AS cst_marital_status, --- NORMALIZING MARITAL STATUS VALUES ---
CASE UPPER(TRIM(cst_gndr))
    WHEN 'M' THEN 'Male'
    WHEN 'F' THEN 'Female'
    ELSE 'Unknown' --- NORMALIZING GENDER VALUES ---
END AS cst_gndr,
cst_create_date
FROM(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
) T WHERE flag_last = 1  --- SELECTING MOST RECENT RECORD PER cst_id ---;

  SET @batch_end_time = GETDATE();
        PRINT '  Loading silver.crm_cust_info completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';


/*
========================================================================
 CLEANING DATA FROM BRONZE LAYER TO SILVER LAYER IN CRM_PRD_INFO 
========================================================================
*/
--- LOADING CLEANED DATA INTO SILVER LAYER IN SILVER.CRM_PRD_INFO ---
SET @batch_start_time = GETDATE();
    PRINT '  Loading silver.crm_prd_info started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> Loading cleaned data into silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, --- EXTRACTING cat_id FROM prd_key ---
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,--- EXTRACTING prd_key FROM prd_key ---
prd_nm,
COALESCE(prd_cost,0) AS prd_cost,--- REPLACING NULL PRD_COST WITH 0 ---
CASE UPPER(TRIM(prd_line))--- STANDARDIZING prd_line VALUES ---
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'T' THEN 'Touring'
    WHEN 'S' THEN 'Other Sales'
    ELSE 'Unknown'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,--- CONVERTING prd_start_dt TO DATE TYPE ---
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info --- CREATING prd_end_dt BASED ON NEXT RECORD'S prd_start_dt ---
  SET @batch_end_time = GETDATE();
        PRINT '  Loading silver.crm_prd_info completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

/*
========================================================================
 CLEANING DATA FROM BRONZE LAYER TO SILVER LAYER IN CRM_SALES_DETAILS
========================================================================
*/
--- LOADING CLEANED DATA INTO SILVER LAYER IN SILVER.CRM_SALES_DETAILS ---
SET @batch_start_time = GETDATE();
    PRINT '  Loading silver.crm_sales_details started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Loading cleaned data into silver.crm_sales_details';
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price)

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE 
    WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
END AS sls_order_dt,---REPLACING INVALID DATES WITH NULL ---
CASE 
    WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 OR sls_ship_dt < sls_order_dt THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
END AS sls_ship_dt,---REPLACING INVALID DATES WITH NULL ---
CASE 
    WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 OR sls_due_dt < sls_order_dt THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
END AS sls_due_dt,---REPLACING INVALID DATES WITH NULL ---
CASE 
    WHEN sls_sales IS NULL OR sls_sales < 0 OR sls_sales!= sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,--- CORRECTING INVALID sls_sales VALUES ---
sls_quantity,
CASE 
    WHEN sls_price IS NULL OR sls_price = 0 THEN ABS(sls_sales / NULLIF(sls_quantity,0)) 
    WHEN sls_price < 0 THEN ABS(sls_price)
    ELSE ABS(sls_price)
END AS sls_price --- CORRECTING INVALID sls_price VALUES ---
FROM bronze.crm_sales_details

  SET @batch_end_time = GETDATE();
        PRINT '  Loading silver.crm_sales_details completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

/*
========================================================================
 CLEANING DATA FROM BRONZE LAYER TO SILVER LAYER IN ERP_CUST_AZ12 
========================================================================
*/
SET @batch_start_time = GETDATE();
    PRINT '  Loading silver.erp_cust_az12 started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Loading cleaned data into silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen)
SELECT 
CASE 
    WHEN LEN(cid)=13 THEN SUBSTRING(cid,4,LEN(cid))
    ELSE cid
END AS cid,
CASE 
    WHEN bdate > GETDATE() THEN NULL
    ELSE bdate
END AS bdate,
CASE 
    WHEN SUBSTRING(UPPER(TRIM(gen)), 1, 1) = 'M' THEN 'Male'
    WHEN SUBSTRING(UPPER(TRIM(gen)), 1, 1) = 'F' THEN 'Female'
    WHEN SUBSTRING(UPPER(TRIM(gen)), 1, 6) = 'FEMALE' THEN 'Female'
    WHEN SUBSTRING(UPPER(TRIM(gen)), 1, 4) = 'MALE' THEN 'Male' 
    ELSE 'Unknown'
END AS gen
FROM bronze.erp_cust_az12

  SET @batch_end_time = GETDATE();
        PRINT '  Loading silver.erp_cust_az12 completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

/*
========================================================================
 CLEANING DATA FROM BRONZE LAYER TO SILVER LAYER IN ERP_LOC_A101
========================================================================
*/
SET @batch_start_time = GETDATE();
    PRINT '  Loading silver.erp_loc_a101 started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> Loading cleaned data into silver.erp_loc_a101';
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry)
SELECT 
REPLACE(cid,'-','') AS cid,
CASE 
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
    WHEN TRIM(cntry) = 'UK' THEN 'United Kingdom'
    WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'Unknown'
    ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101

  SET @batch_end_time = GETDATE();
        PRINT '  Loading silver.erp_loc_a101 completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

/*
========================================================================
 CLEANING DATA FROM BRONZE LAYER TO SILVER LAYER IN ERP_PX_CAT_G1V2
========================================================================
*/
SET @batch_start_time = GETDATE();
    PRINT '  Loading silver.erp_px_cat_g1v2 started at: ' + CONVERT(NVARCHAR, @batch_start_time, 120);
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Loading cleaned data into silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance)
SELECT 
id,
cat,
subcat,
CASE 
    WHEN SUBSTRING(TRIM(maintenance),1,3) = 'Yes' THEN 'Yes'
    WHEN SUBSTRING(TRIM(maintenance),1,2) = 'No' THEN 'No'
    ELSE 'Unknown'
END AS maintenance
FROM bronze.erp_px_cat_g1v2

  SET @batch_end_time = GETDATE();
        PRINT '  Loading silver.erp_px_cat_g1v2 completed at: ' + CONVERT(NVARCHAR, @batch_end_time, 120);    
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

END TRY 
    BEGIN CATCH 
        PRINT '=========================================='
            PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
            PRINT 'Error Message' + ERROR_MESSAGE();
            PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
            PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
            PRINT '=========================================='
	END CATCH
END

-- VALIDATING CLEANED DATA IN SILVER LAYER --
EXEC silver.load_silver;
SELECT * FROM silver.crm_cust_info;
SELECT * FROM silver.crm_prd_info;
SELECT * FROM silver.crm_sales_details;
SELECT * FROM silver.erp_loc_a101;
SELECT * FROM silver.erp_px_cat_g1v2;
SELECT * FROM silver.erp_cust_az12;

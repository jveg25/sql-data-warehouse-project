/*
=====================================================================

Stored Procedure: Load Silver Layer (Bronze -> Silver)

=====================================================================

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.
    Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;

=====================================================================
*/

CALL silver.load_silver();

DROP PROCEDURE IF EXISTS silver.load_silver;

CREATE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$

DECLARE
start_time TIMESTAMP;
end_time TIMESTAMP;
duration INTERVAL;
batch_start_time TIMESTAMP;
batch_end_time TIMESTAMP;
batch_duration INTERVAL;

BEGIN

	BEGIN

		batch_start_time := clock_timestamp(); 
		
		-- =========================================
		-- INSERT DATA INTO silver.crm_cust_info
		-- =========================================
		
	    RAISE NOTICE '===============================';
	    RAISE NOTICE 'Loading Silver Layer';
	    RAISE NOTICE '===============================';

        RAISE NOTICE '-------------------------------';
        RAISE NOTICE ' Loading CRM Tables';
        RAISE NOTICE '-------------------------------';

		start_time := clock_timestamp(); 
		
		RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		RAISE NOTICE '>> Inserting data into: silver.crm_cust_info';
		
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
			)
		
		SELECT 
			cst_id,
			
			TRIM(cst_key) AS cst_key,
			
			TRIM(cst_firstname) AS cst_firstname,
			
			TRIM(cst_lastname) AS cst_lastname,
		
			CASE WHEN TRIM(cst_marital_status) = 'S' THEN 'Single'
				WHEN TRIM(cst_marital_status) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status, --Normalize marital status values to readable format
			
			CASE WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
				WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
				ELSE 'n/a'
			END cst_gndr, --Normalize gender values to readable format
			
			cst_create_date
		
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer

		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';
		
		-- =========================================
		-- INSERT DATA INTO silver.crm_prd_info
		-- =========================================

		start_time := clock_timestamp(); 
		
		RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		RAISE NOTICE '>> Inserting data into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)
		
		SELECT
			prd_id,
			
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			
			SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
			
			prd_nm,
			
			COALESCE(prd_cost,0) AS prd_cost,
		
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line, --Normalize gender values to readable format
				
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
			
		FROM bronze.crm_prd_info;

		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';
		
		-- =========================================
		-- INSERT DATA INTO silver.crm_sales_details
		-- =========================================

		start_time := clock_timestamp(); 
		
		RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		RAISE NOTICE '>> Inserting data into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
			)
		
		SELECT 
			TRIM(sls_ord_num) AS sls_ord_num,
			
			TRIM(sls_prd_key) AS sls_prd_key,
			
			sls_cust_id,
			
			CASE WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
				ELSE TO_DATE(CAST(sls_order_dt AS VARCHAR), 'YYYYMMDD')
			END AS sls_order_dt,
		
			CASE WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
				ELSE TO_DATE(CAST(sls_ship_dt AS VARCHAR), 'YYYYMMDD')
			END AS sls_ship_dt,
		
			CASE WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
				ELSE TO_DATE(CAST(sls_due_dt AS VARCHAR), 'YYYYMMDD')
			END AS sls_due_dt,
			
			CASE WHEN sls_sales <= 0 OR sls_sales IS NULL THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
			END AS sls_sales,
		
			sls_quantity,
		
			CASE WHEN sls_price = 0 OR sls_price IS NULL THEN ABS(sls_sales) / COALESCE(sls_quantity,0)
			WHEN sls_price < 0 THEN ABS(sls_price)
			ELSE sls_price
			END AS sls_price
			
		FROM bronze.crm_sales_details;

		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';
		
		-- =========================================
		-- INSERT DATA INTO silver.erp_cust_az12
		-- =========================================

		start_time := clock_timestamp(); 
		
		RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		RAISE NOTICE '>> Inserting data into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		
		SELECT
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
				ELSE cid
			END AS cid,
		
			CASE WHEN bdate > CURRENT_DATE THEN NULL
				ELSE bdate
			END AS bdate,
			CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen
		FROM bronze.erp_cust_az12;

		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';
		
		-- =========================================
		-- INSERT DATA INTO silver.erp_loc_a101
		-- =========================================

		start_time := clock_timestamp(); 
		
		RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		RAISE NOTICE '>> Inserting data into: silver.erp_loc_a101';
		
		INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
		)
		
		SELECT
			REPLACE(cid, '-', '') AS cid,
			CASE WHEN UPPER(TRIM(cntry)) IN ('GERMANY', 'GE') THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ('AUSTRALIA') THEN 'Australia'
				WHEN UPPER(TRIM(cntry)) IN ('US', 'UNITED STATES', 'USA') THEN 'United States'
				WHEN UPPER(TRIM(cntry)) IN ('CANADA') THEN 'Canada'
				WHEN UPPER(TRIM(cntry)) IN ('FRANCE') THEN 'France'
				WHEN UPPER(TRIM(cntry)) IN ('UNITED KINGDOM') THEN 'United Kingdom'
				ELSE 'n/a'
			END AS cntry
		FROM bronze.erp_loc_a101;
		
		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';
		
		-- =========================================
		-- INSERT DATA INTO silver.erp_px_cat_g1v2
		-- =========================================

		start_time := clock_timestamp(); 
		
		RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		RAISE NOTICE '>> Inserting data into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;

		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';


		batch_end_time := clock_timestamp(); 
		batch_duration := batch_end_time - batch_start_time;
		
        RAISE NOTICE '===============================';
        RAISE NOTICE 'Silver Layer Loaded Successfully!';
		RAISE NOTICE 'Total Load Duration: % seconds ', EXTRACT(SECONDS FROM batch_duration);
        RAISE NOTICE '===============================';


    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '===============================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
            RAISE NOTICE 'Error message: %', SQLERRM;
            RAISE NOTICE 'SQL state: %', SQLSTATE;
            RAISE NOTICE '===============================';
    END;

END;
$$;

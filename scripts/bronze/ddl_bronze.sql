/*
====================================================================

DDL Script: Create Bronze Tables

Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables

====================================================================
*/

CALL bronze.load_bronze();

DROP PROCEDURE IF EXISTS bronze.load_bronze;

CREATE PROCEDURE bronze.load_bronze()
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
	    RAISE NOTICE '===============================';
	    RAISE NOTICE 'Loading Bronze Layer';
	    RAISE NOTICE '===============================';

        RAISE NOTICE '-------------------------------';
        RAISE NOTICE ' Loading CRM Tables';
        RAISE NOTICE '-------------------------------';
		
		start_time := clock_timestamp(); 
        
		RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
        COPY bronze.crm_cust_info
        FROM '/Users/jveg/Courses/13_yt_sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');
		
		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';

		start_time := clock_timestamp(); 
		
        RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
        COPY bronze.crm_sales_details
        FROM '/Users/jveg/Courses/13_yt_sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';

		start_time := clock_timestamp(); 
		
        RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
        COPY bronze.crm_prd_info
        FROM '/Users/jveg/Courses/13_yt_sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';

        RAISE NOTICE '-------------------------------';
        RAISE NOTICE ' Loading ERP Tables';
        RAISE NOTICE '-------------------------------';

		start_time := clock_timestamp(); 
		
        RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
        COPY bronze.erp_cust_az12
        FROM '/Users/jveg/Courses/13_yt_sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';

		start_time := clock_timestamp(); 
		
        RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
        COPY bronze.erp_loc_a101
        FROM '/Users/jveg/Courses/13_yt_sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';

		start_time := clock_timestamp(); 
		
        RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        COPY bronze.erp_px_cat_g1v2
        FROM '/Users/jveg/Courses/13_yt_sql-data-warehouse-project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

		end_time := clock_timestamp(); 
		duration := end_time - start_time;
		
		RAISE NOTICE '>> Load Duration: % seconds ', EXTRACT(SECONDS FROM duration);
		RAISE NOTICE '>> --------------------';

		batch_end_time := clock_timestamp(); 
		batch_duration := batch_end_time - batch_start_time;
		
        RAISE NOTICE '===============================';
        RAISE NOTICE 'Bronze Layer Loaded Successfully!';
		RAISE NOTICE 'Total Load Duration: % seconds ', EXTRACT(SECONDS FROM batch_duration);
        RAISE NOTICE '===============================';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE '===============================';
            RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
            RAISE NOTICE 'Error message: %', SQLERRM;
            RAISE NOTICE 'SQL state: %', SQLSTATE;
            RAISE NOTICE '===============================';
    END;

END;
$$;

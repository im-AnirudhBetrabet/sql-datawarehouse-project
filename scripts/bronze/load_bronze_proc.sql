/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE 
			@start_time DATETIME,
			@end_time DATETIME, 
			@batch_starttime DATETIME, 
			@batch_endtime DATETIME;
BEGIN
	TRY
		SET @batch_starttime = GETDATE();
		/* crm_cust_info load starts */
		PRINT '======================================';
		PRINT 'Loading Bronze Layer';
		PRINT '======================================';
		PRINT '--------------------------------------';
		PRINT 'Loading CRM Customer Info table';
		PRINT '--------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting data into table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM "C:\Users\Anirudh\OneDrive\Desktop\Anirudh\sql-datawarehouse-project\datasets\crm\cust_info.csv"
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		/* crm_cust_info load ends*/

		/* crm_prd_info table load begins*/
		PRINT '--------------------------------------';
		PRINT 'Loading CRM Product Info table';
		PRINT '--------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting data into table: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM "C:\Users\Anirudh\OneDrive\Desktop\Anirudh\sql-datawarehouse-project\datasets\crm\prd_info.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		/* crm_prd_info table load ends */

		/* crm_sales_details table load begins*/
		PRINT '--------------------------------------';
		PRINT 'Loading CRM Sales Details table';
		PRINT '--------------------------------------';
		SET @start_time = GETDATE();
		PRINT '--------------------------------------';
		PRINT '>> Truncating table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting data into table: bronze.crm_sales_details';
		PRINT '--------------------------------------';
		BULK INSERT bronze.crm_sales_details
		FROM "C:\Users\Anirudh\OneDrive\Desktop\Anirudh\sql-datawarehouse-project\datasets\crm\sales_details.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		/* crm_sales_details table load ends*/

		/* erp_cust_az12 table load starts*/
		PRINT '--------------------------------------';
		PRINT 'Loading ERP Customer AZ12 table';
		PRINT '--------------------------------------';
		SET @start_time = GETDATE();
		PRINT '--------------------------------------';
		PRINT '>> Truncating table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting data into table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM "C:\Users\Anirudh\OneDrive\Desktop\Anirudh\sql-datawarehouse-project\datasets\erp\cust_az12.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		/* erp_cust_az12 table load ends*/

		/* erp_loc_a101 table load begins*/
		PRINT '--------------------------------------';
		PRINT 'Loading ERP Location A101 table';
		PRINT '--------------------------------------';
		SET @start_time = GETDATE();
		PRINT '--------------------------------------';
		PRINT '>> Truncating table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting data into table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM "C:\Users\Anirudh\OneDrive\Desktop\Anirudh\sql-datawarehouse-project\datasets\erp\loc_a101.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		/* erp_loc_a101 table load ends */

		/* erp_px_cat_g1v2 table load begins*/
		PRINT '--------------------------------------';
		PRINT 'Loading ERP Product Category g1v2 table';
		PRINT '--------------------------------------';
		Set @start_time = GETDATE();
		PRINT '--------------------------------------';
		PRINT '>> Truncating table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting data into table: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM "C:\Users\Anirudh\OneDrive\Desktop\Anirudh\sql-datawarehouse-project\datasets\erp\px_cat_g1v2.csv"
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		)
		PRINT '--------------------------------------';
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------------------';
		/* erp_px_cat_g1v2 table load ends*/
		SET @batch_endtime = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_starttime, @batch_endtime) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
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
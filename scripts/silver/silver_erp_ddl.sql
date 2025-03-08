/*
===============================================================================
DDL Script: Create silver ERP Tables
===============================================================================
Script Purpose:
    This script creates erp tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

------- Creating cust az12 table -------
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12(
	cid	  NVARCHAR(50),
	dbate DATE,
	gen   NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

------- Creating loc A101 table -------
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101(
	cid   NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

------- Creating loc A101 table -------
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.px_cat_G1V2;
GO

CREATE TABLE silver.erp_px_cat_g1v2(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	sub_cat		NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
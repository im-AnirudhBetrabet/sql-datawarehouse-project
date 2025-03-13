/*
===============================================================================
DDL Script: Create Bronze ERP Tables
===============================================================================
Script Purpose:
    This script creates erp tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

------- Creating cust az12 table -------
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12(
	cid	  NVARCHAR(50),
	bdate DATE,
	gen   NVARCHAR(50)
);

------- Creating loc A101 table -------
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101(
	cid   NVARCHAR(50),
	cntry NVARCHAR(50)
);

------- Creating loc A101 table -------
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	sub_cat		NVARCHAR(50),
	maintenance NVARCHAR(50)
);
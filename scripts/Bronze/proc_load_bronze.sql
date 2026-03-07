*/This script purpose is to load the tables in bronze schema from source systems 
*----------------------------------------------------------*
 *This script Uses Bulk insert method to load the data from sources
 This script calculate the loading duration for each table and for entire batch load 
This script uses variables and try catch methods to identify any errors 
Entire script was stored in a procedure bronze.load_bronze

Usage: EXEC bronze.load_bronze
  */-----------------------------------------------------------*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
DECLARE @start_time DATETIME,@end_time DATETIME, @batchstart_time DATETIME, @batchend_time DATETIME;
BEGIN TRY
    SET @batchstart_time=GETDATE();
	PRINT'==============================================================================';
	PRINT'LOADING BRONZE LAYER';
	PRINT'==============================================================================';


	PRINT'==============================================================================';
	PRINT'LOADING CRM TABLES';
	PRINT'==============================================================================';

	PRINT'>>>>>>>>>TRUNCATING bronze.crm_cust_info TABLE>>>>>>>>>>>>>>>>>';
	SET @start_time=GETDATE();

	TRUNCATE TABLE bronze.crm_cust_info;

	PRINT'>>>>>>>>>>INSERTING DATA INTO bronze.crm_cust_info TABLE>>>>';

			BULK INSERT bronze.crm_cust_info FROM 'C:\Users\Reshmikha\OneDrive\Data with Barra\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH(
			   FIRSTROW = 2,
			   FIELDTERMINATOR=',',
			   TABLOCK
			   );
set @end_time=GETDATE();
print'LOAD DURATION:' +cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) +'seconds';
print'-----------';
	PRINT'>>>>>>>>>TRUNCATING bronze.crm_prd_info  TABLE>>>>>>>>>>>>>>>>>';

	SET @start_time=GETDATE();

		TRUNCATE TABLE bronze.crm_prd_info;
	PRINT'>>>>>>>>>>INSERTING DATA INTO bronze.crm_prd_info TABLE>>>>';

	BULK INSERT bronze.crm_prd_info FROM 'C:\Users\Reshmikha\OneDrive\Data with Barra\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
set @end_time=GETDATE();
print'LOAD DURATION:' +cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) +'seconds';
print'-----------';

PRINT'>>>>>>>>>TRUNCATING bronze.crm_sales_details TABLE>>>>>>>>>>>>>>>>>';

SET @start_time=GETDATE();
TRUNCATE TABLE bronze.crm_sales_details;
PRINT'>>>>>>>>>>INSERTING DATA INTO bronze.crm_sales_details TABLE>>>>';	 
BULK INSERT bronze.crm_sales_details FROM 'C:\Users\Reshmikha\OneDrive\Data with Barra\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH(
 FIRSTROW = 2,
FIELDTERMINATOR=',',
TABLOCK
);
set @end_time=GETDATE();
print'LOAD DURATION:' +cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) +'seconds'
print'-----------';
	PRINT'==============================================================================';
	PRINT'LOADING ERP TABLES';
	PRINT'==============================================================================';

	PRINT'>>>>>>>>>TRUNCATIONG bronze.erp_CUST_AZ12 TABLE>>>>>>>>>>>>>>>>>';
	SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
	PRINT'>>>>>>>>>INSERTING DATA INTO bronze.erp_CUST_AZ12>>>>>>>>>>';

			BULK INSERT bronze.erp_CUST_AZ12 FROM 'C:\Users\Reshmikha\OneDrive\Data with Barra\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH(
			   FIRSTROW = 2,
			   FIELDTERMINATOR=',',
			   TABLOCK
			   );
set @end_time=GETDATE();
print'LOAD DURATION:' +cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) +'seconds';
print'-----------';
		----------------------------------------------------------------------------
PRINT'>>>>>>>>>TRUNCATING bronze.erp_LOC_A101 TABLE>>>>>>>>>>>>>>>>>';
SET @start_time=GETDATE();
TRUNCATE TABLE bronze.erp_LOC_A101;

PRINT'>>>>>>>>>INSERTING DATA INTO bronze.erp_LOC_A101 TABLE>>>>>>>>>>';

			BULK INSERT bronze.erp_LOC_A101 FROM 'C:\Users\Reshmikha\OneDrive\Data with Barra\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH(
			   FIRSTROW = 2,
			   FIELDTERMINATOR=',',
			   TABLOCK
			   );

set @end_time=GETDATE();
print'LOAD DURATION:' +cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) +'seconds';
print'-----------';
	PRINT'>>>>>>>>>TRUNCATING bronze.erp_PX_CAT_G1V2 TABLE>>>>>>>>>>>>>>>>>';
SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

	PRINT'>>>>>>>>>INSERTING DATA INTO bronze.erp_PX_CAT_G1V2 TABLE>>>>>>>>>>';

			BULK INSERT bronze.erp_PX_CAT_G1V2 FROM 'C:\Users\Reshmikha\OneDrive\Data with Barra\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH(
			   FIRSTROW = 2,
			   FIELDTERMINATOR=',',
			   TABLOCK
			   );
set @end_time=GETDATE();
print'LOAD DURATION:' +cast(DATEDIFF(SECOND,@start_time,@end_time) as NVARCHAR) +'seconds';
print'-----------';
set @batchend_time = GETDATE();

print'TOTAL DURATION TO LOAD WHOLE BRONZE LAYER:' +cast(DATEDIFF(SECOND,@batchstart_time,@batchend_time) as NVARCHAR) +'seconds';

		   END TRY
		   BEGIN CATCH
		   PRINT'=================================';
		   PRINT'ERRORS WHILE LOADING BRONZE TABLES';
		   PRINT'ERROR_MESSAGE'+ ERROR_MESSAGE();
		   PRINT'ERROR_MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		   PRINT'ERROR_MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR);
		   PRINT'=================================';
		   END CATCH
END
  
  

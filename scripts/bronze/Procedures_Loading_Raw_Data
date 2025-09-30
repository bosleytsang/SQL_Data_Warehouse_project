/*
===================================================================
Stored Procedure: Load Raw Data 
===================================================================
Purpose: 
    This stored procedured loads the data into the 'bronze'schema from external csv files. 
    It performs the following actions: 
    1. Truncates the tables brfore loading data
    2. Use 'Bulk Insert' to load data from csv into Tables 
*/

CREATE OR ALTER PROCEDURE bronze.load_procedure AS
BEGIN
    PRINT'================================'
    PRINT 'Loading Bronze Layer'
     PRINT'================================'

     PRINT'--------------------------------'
    PRINT 'Loading CRM Tables'
     PRINT'--------------------------------'
    TRUNCATE TABLE bronze.crm_cust_info;
    BULK INSERT bronze.crm_cust_info
    FROM '/var/opt/mssql/csv/datasets/source_crm/cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );

    TRUNCATE TABLE bronze.crm_prd_info;
    BULK INSERT bronze.crm_prd_info
    FROM '/var/opt/mssql/csv/datasets/source_crm/prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );

    TRUNCATE TABLE bronze.crm_sales_details;
    BULK INSERT bronze.crm_sales_details
    FROM '/var/opt/mssql/csv/datasets/source_crm/sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );

    PRINT'--------------------------------'
    PRINT 'Loading ERP Tables'
    PRINT'--------------------------------'

    TRUNCATE TABLE bronze.erp_cust_az12;
    BULK INSERT bronze.erp_cust_az12
    FROM '/var/opt/mssql/csv/datasets/source_erp/CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );

    TRUNCATE TABLE bronze.erp_loc_a101;
    BULK INSERT bronze.erp_loc_a101
    FROM '/var/opt/mssql/csv/datasets/source_erp/LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    BULK INSERT bronze.erp_px_cat_g1v2
    FROM '/var/opt/mssql/csv/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        TABLOCK
    );
END

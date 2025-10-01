-- Create database and tables in SQL Server

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME2 default GETDATE()
);

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    cat_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    prd_name NVARCHAR(100),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 default GETDATE()
);

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details; 
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT, 
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 default GETDATE()
);

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 default GETDATE()
);

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 default GETDATE()
);

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 default GETDATE()
);

-- Insert Cleaned Data into Silver Layer Tables

CREATE OR ALTER PROCEDURE silver.load_procedure
AS
BEGIN

TRUNCATE TABLE silver.crm_cust_info;
PRINT 'Inserting data into silver.crm_cust_info';
INSERT into silver.crm_cust_info(
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
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname) AS cst_lastname,
  CASE WHEN UPPER(trim(cst_marital_status)) = 's' THEN 'Single'   
       WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
       ELSE 'n/a' END AS cst_marital_status,
  CASE WHEN UPPER(trim(cst_gndr)) = 'M' THEN 'Male'   
       WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' 
       ELSE 'n/a' END AS cst_gndr,
  cst_create_date
FROM(
    SELECT * ,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) tmp
WHERE flag_last = 1 ;

TRUNCATE TABLE silver.crm_prd_info;
PRINT 'Inserting data into silver.crm_prd_info';
INSERT silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_name,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id, 
    replace(SUBSTRING (prd_key,1,5),'-','_') as cat_id,
    SUBSTRING (prd_key,7,Len(prd_key)) AS prd_key,
    prd_name, 
    ISNULL(prd_cost,0) as prd_cost, 
    CASE WHEN UPPER(trim(prd_line)) = 'M' THEN 'Mountain'
         WHEN UPPER(trim(prd_line)) = 'R' THEN 'Road'
         WHEN UPPER(trim(prd_line)) = 'T' THEN 'Touring'
         WHEN UPPER(trim(prd_line)) = 'S' THEN 'Other Sales'
         ELSE 'n/a' END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(Lead(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 as date) AS prd_end_dt
FROM bronze.crm_prd_info;

TRUNCATE TABLE silver.crm_sales_details;
PRINT 'Inserting data into silver.crm_sales_details';
INSERT INTO silver.crm_sales_details
(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_price,
    sls_quantity
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR len(sls_order_dt) != 8 THEN NULL
            ELSE cast(cast(sls_order_dt as varchar(8)) as date ) end as sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR len(sls_ship_dt) != 8 THEN NULL
            ELSE cast(cast(sls_ship_dt as varchar(8)) as date ) end as sls_ship_dt,
    
    CASE WHEN sls_due_dt = 0 OR len(sls_due_dt) != 8 THEN NULL
            ELSE cast(cast(sls_due_dt as varchar(8)) as date ) end as sls_due_dt,
    CASE WHEN sls_sales is NULL OR sls_sales <=0 OR sls_sales != sls_quantity * Abs(sls_price)
                THEN sls_quantity * Abs(sls_price)
            ELSE sls_sales END as sls_sales,
    CASE WHEN sls_price is NULL OR sls_price <= 0 THEN sls_sales/nullif(sls_quantity, 0)
            ELSE sls_price END as sls_price,
    sls_quantity
FROM bronze.crm_sales_details;


TRUNCATE TABLE silver.erp_cust_az12;
PRINT 'Inserting data into silver.erp_cust_az12';
insert into silver.erp_cust_az12 (cid,bdate,gen)
SELECT 
    CASE WHEN cid like 'NAS%' THEN substring(cid,4,len(cid))
        else cid
        end as cid,
    bdate, 
    CASE WHEN upper(trim(gen)) in ('M','MALE') THEN 'Male'
        WHEN upper(trim(gen)) in ('F', 'FEMALE')  THEN 'Female'
        ELSE 'n/a' END AS gen
FROM  bronze.erp_cust_az12;

TRUNCATE TABLE silver.erp_loc_a101;
PRINT 'Inserting data into silver.erp_loc_a101';
INSERT into silver.erp_loc_a101 (cid,cntry)
select 
    REPLACE(cid,'-','') as cid,
    case when trim(cntry) = 'DE' then 'Germany'
         when trim(cntry) in ('US','USA') then 'United States'
         when trim(cntry)= '' or cntry is null then 'n/a'
         else trim(cntry)
    end as cntry
from bronze.erp_loc_a101 ;

TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT 'Inserting data into silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;

END;
GO

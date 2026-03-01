/*
===============================================================================
Stored Procedure: bronze.load_bronce
Author: Data Engineering Project
Layer: Bronze
Description:
    Loads raw CSV files into Bronze layer tables using BULK INSERT.

    ✔ Cleans target tables (full reload approach)
    ✔ Measures execution time per table
    ✔ Logs row counts
    ✔ Handles errors with TRY/CATCH
    ✔ Uses parametrized base path (portable across environments)

Usage Example:
    EXEC bronze.load_bronce 
        @base_path = 'C:\warehouse\datasets';

Important:
    The @base_path folder must contain:

    \source_crm\cust_info.csv
    \source_crm\prd_info.csv
    \source_crm\sales_details.csv
    \source_erp\LOC_A101.csv
    \source_erp\CUST_AZ12.csv
    \source_erp\PX_CAT_G1V2.csv
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronce
    @base_path NVARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @rows INT,
        @file_path NVARCHAR(1000),
        @sql NVARCHAR(MAX);

    BEGIN TRY

        PRINT '=============================================';
        PRINT 'INICIO PROCESO CARGA CAPA BRONZE: ' + CONVERT(VARCHAR, GETDATE(), 120);
        PRINT '=============================================';

        -------------------------------------------------
        -- 1. CLEAN TARGET TABLES (FULL RELOAD STRATEGY)
        -------------------------------------------------
        PRINT 'Cleaning Bronze tables...';

        TRUNCATE TABLE bronze.crm_cust_info;
        TRUNCATE TABLE bronze.crm_prd_info;
        TRUNCATE TABLE bronze.crm_sales_details;
        TRUNCATE TABLE bronze.erp_loc_a101;
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT 'Tables cleaned successfully.';
        PRINT '---------------------------------------------';


        -------------------------------------------------
        -- 2. LOAD: CRM CUSTOMER
        -------------------------------------------------
        PRINT '>>> START crm_cust_info load';

        SET @file_path = @base_path + '\source_crm\cust_info.csv';
        SET @start_time = GETDATE();

        SET @sql = '
        BULK INSERT bronze.crm_cust_info
        FROM ''' + @file_path + '''
        WITH (
            FORMAT = ''CSV'',
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            TABLOCK
        );';

        EXEC sp_executesql @sql;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '<<< END crm_cust_info load';
        PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR);
        PRINT 'Duration (seconds): ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR);
        PRINT '---------------------------------------------';


        -------------------------------------------------
        -- 3. LOAD: CRM PRODUCT
        -------------------------------------------------
        PRINT '>>> START crm_prd_info load';

        SET @file_path = @base_path + '\source_crm\prd_info.csv';
        SET @start_time = GETDATE();

        SET @sql = '
        BULK INSERT bronze.crm_prd_info
        FROM ''' + @file_path + '''
        WITH (
            FORMAT = ''CSV'',
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            TABLOCK
        );';

        EXEC sp_executesql @sql;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '<<< END crm_prd_info load';
        PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR);
        PRINT 'Duration (seconds): ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR);
        PRINT '---------------------------------------------';


        -------------------------------------------------
        -- 4. LOAD: CRM SALES
        -------------------------------------------------
        PRINT '>>> START crm_sales_details load';

        SET @file_path = @base_path + '\source_crm\sales_details.csv';
        SET @start_time = GETDATE();

        SET @sql = '
        BULK INSERT bronze.crm_sales_details
        FROM ''' + @file_path + '''
        WITH (
            FORMAT = ''CSV'',
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            TABLOCK
        );';

        EXEC sp_executesql @sql;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '<<< END crm_sales_details load';
        PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR);
        PRINT 'Duration (seconds): ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR);
        PRINT '---------------------------------------------';


        -------------------------------------------------
        -- 5. LOAD: ERP LOCATION
        -------------------------------------------------
        PRINT '>>> START erp_loc_a101 load';

        SET @file_path = @base_path + '\source_erp\LOC_A101.csv';
        SET @start_time = GETDATE();

        SET @sql = '
        BULK INSERT bronze.erp_loc_a101
        FROM ''' + @file_path + '''
        WITH (
            FORMAT = ''CSV'',
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            TABLOCK
        );';

        EXEC sp_executesql @sql;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '<<< END erp_loc_a101 load';
        PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR);
        PRINT 'Duration (seconds): ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR);
        PRINT '---------------------------------------------';


        -------------------------------------------------
        -- 6. LOAD: ERP CUSTOMER
        -------------------------------------------------
        PRINT '>>> START erp_cust_az12 load';

        SET @file_path = @base_path + '\source_erp\CUST_AZ12.csv';
        SET @start_time = GETDATE();

        SET @sql = '
        BULK INSERT bronze.erp_cust_az12
        FROM ''' + @file_path + '''
        WITH (
            FORMAT = ''CSV'',
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            TABLOCK
        );';

        EXEC sp_executesql @sql;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '<<< END erp_cust_az12 load';
        PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR);
        PRINT 'Duration (seconds): ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR);
        PRINT '---------------------------------------------';


        -------------------------------------------------
        -- 7. LOAD: ERP CATEGORY
        -------------------------------------------------
        PRINT '>>> START erp_px_cat_g1v2 load';

        SET @file_path = @base_path + '\source_erp\PX_CAT_G1V2.csv';
        SET @start_time = GETDATE();

        SET @sql = '
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM ''' + @file_path + '''
        WITH (
            FORMAT = ''CSV'',
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            TABLOCK
        );';

        EXEC sp_executesql @sql;

        SET @rows = @@ROWCOUNT;
        SET @end_time = GETDATE();

        PRINT '<<< END erp_px_cat_g1v2 load';
        PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR);
        PRINT 'Duration (seconds): ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS VARCHAR);
        PRINT '---------------------------------------------';


        PRINT '=============================================';
        PRINT 'BRONZE LOAD COMPLETED SUCCESSFULLY';
        PRINT '=============================================';

    END TRY
    BEGIN CATCH

        PRINT '=============================================';
        PRINT 'ERROR DURING BRONZE LOAD';
        PRINT ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
        PRINT '=============================================';

    END CATCH
END;
GO

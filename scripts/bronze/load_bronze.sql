-- =============================================
-- Description: Truncates and bulk loads a single bronze table from a CSV file.
-- =============================================
CREATE OR ALTER PROCEDURE dbo.usp_LoadBronzeTable
    @SchemaName NVARCHAR(128),
    @TableName NVARCHAR(128),
    @FilePath NVARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SqlStatement NVARCHAR(MAX);
    DECLARE @FullTableName NVARCHAR(257) = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);
    DECLARE @StartTime DATETIME2 = GETDATE();

    PRINT '--------------------------------------------------';
    PRINT 'LOADING TABLE: ' + @FullTableName;
    PRINT 'FROM FILE: ' + @FilePath;

    -- Build the dynamic SQL statement to avoid SQL injection
    SET @SqlStatement = N'
        TRUNCATE TABLE ' + @FullTableName + ';

        BULK INSERT ' + @FullTableName + '
        FROM ''' + @FilePath + '''
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            TABLOCK
        );';

    -- Execute the dynamic SQL
    EXEC sp_executesql @SqlStatement;

    DECLARE @DurationSeconds DECIMAL(10, 2) = CAST(DATEDIFF(millisecond, @StartTime, GETDATE()) AS DECIMAL(10, 2)) / 1000;
    PRINT 'LOAD DURATION: ' + CAST(@DurationSeconds AS VARCHAR(20)) + ' seconds';
    PRINT '--------------------------------------------------';
END
GO

-- =============================================
-- Description: Main procedure to load all bronze layer tables.
--              Wraps all loads in a single transaction for atomicity.
-- =============================================
CREATE OR ALTER PROCEDURE bronze.usp_LoadBronze
AS
BEGIN
    SET NOCOUNT ON;

    -- Start a transaction to ensure all loads succeed or fail together
    BEGIN TRANSACTION;

    BEGIN TRY
        -- Execute the helper procedure for each table
        EXEC dbo.usp_LoadBronzeTable @SchemaName = 'bronze', @TableName = 'crm_cust_info', @FilePath = '/sql-data/source_crm/cust_info.csv';
        EXEC dbo.usp_LoadBronzeTable @SchemaName = 'bronze', @TableName = 'crm_prd_info', @FilePath = '/sql-data/source_crm/prd_info.csv';
        EXEC dbo.usp_LoadBronzeTable @SchemaName = 'bronze', @TableName = 'crm_sae_details', @FilePath = '/sql-data/source_crm/sales_details.csv';
        EXEC dbo.usp_LoadBronzeTable @SchemaName = 'bronze', @TableName = 'erp_cust_az12', @FilePath = '/sql-data/source_erp/CUST_AZ12.csv';
        EXEC dbo.usp_LoadBronzeTable @SchemaName = 'bronze', @TableName = 'erp_loc_a101', @FilePath = '/sql-data/source_erp/LOC_A101.csv';
        EXEC dbo.usp_LoadBronzeTable @SchemaName = 'bronze', @TableName = 'erp_px_cat_g1v2', @FilePath = '/sql-data/source_erp/PX_CAT_G1V2.csv';

        -- If all executions were successful, commit the transaction
        COMMIT TRANSACTION;
        PRINT 'SUCCESS: All bronze tables loaded successfully.';

    END TRY
    BEGIN CATCH
        -- If an error occurs, roll back the entire transaction
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Re-throw the original error to the caller
        THROW;
    END CATCH;
END
GO

-- How to run the main procedure
EXEC bronze.usp_LoadBronze;

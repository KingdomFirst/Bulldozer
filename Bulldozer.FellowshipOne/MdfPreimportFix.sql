/************************************************************
****************** Fixes MDF storage pointers ***************
************************************************************/

SET NOCOUNT ON;
DECLARE @tableName VARCHAR(200);
SET @tableName = '';
WHILE EXISTS
(	
    --Find all child tables and those which have no relations
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME > @tableName
)
BEGIN
    SELECT @tableName = MIN(TABLE_NAME)
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME > @tableName;
    
	PRINT 'Starting cleanup process on '+@tableName;

    -- Backup table
    EXEC ('SELECT * INTO '+@tableName+'_bak FROM '+@tableName);
        
    -- Truncate the table
    EXEC ('TRUNCATE TABLE '+@tableName);

    -- Fix boolean fields in respective tables
    IF( @tableName = 'ActivityMinistry' )
        BEGIN
            PRINT 'Setting boolean fields on '+@tableName;
            EXEC ('ALTER TABLE '+@tableName+' ALTER COLUMN Ministry_Active VARCHAR(30) NOT NULL');
            EXEC ('ALTER TABLE '+@tableName+' ALTER COLUMN Activity_Active VARCHAR(30) NOT NULL');
        END;

    -- Restore backup
    PRINT 'Restoring data to '+@tableName;
    EXEC ('INSERT '+@tableName+' SELECT * FROM '+@tableName+'_bak');

    -- Drop backup
    EXEC ('DROP TABLE '+@tableName+'_bak');
END;
USE AdventureWorksDW2022;
GO

SELECT
    name,
    is_cdc_enabled
FROM sys.databases
WHERE name = 'AdventureWorksDW2022';

name	is_cdc_enabled
AdventureWorksDW2022	1

EXEC sys.sp_cdc_help_change_data_capture;
GO
source_schema	source_table	capture_instance	object_id	source_object_id	start_lsn	end_lsn	supports_net_changes	has_drop_pending	role_name	index_name	filegroup_name	create_date	index_column_list	captured_column_list
dbo	CDC_LearningCustomer	dbo_CDC_LearningCustomer	2018106230	1618104805	0x0000002B000022180001	NULL	1	NULL	NULL	PK__CDC_Lear__A4AE64B8DCC90A41	NULL	2026-11-02 21:09:11.500	[CustomerID]	[CustomerID], [CustomerName], [City], [Email], [UpdatedAt]


USE master;
GO

-- -- Create the login for debezium user
CREATE LOGIN debezium_user
WITH PASSWORD = 'debezium_user';
GO

drop login debezium_user;

USE AdventureWorksDW2022;
GO

-- Create the database user
CREATE USER debezium_user
FOR LOGIN debezium_user;
GO

-- Grant required permissions
USE AdventureWorksDW2022;
GO

-- Read the source table for initial snapshot
GRANT SELECT
ON OBJECT::dbo.CDC_LearningCustomer
TO debezium_user;
GO

-- Read CDC change tables and metadata
GRANT SELECT ON SCHEMA::cdc
TO debezium_user;
GO

GRANT EXECUTE ON SCHEMA::cdc
TO debezium_user;
GO

-- Read database state
GRANT VIEW DATABASE STATE
TO debezium_user;
GO

-- Read table metadata
GRANT VIEW DEFINITION
ON OBJECT::dbo.CDC_LearningCustomer
TO debezium_user;
GO

USE master;
GO

GRANT VIEW SERVER STATE
TO debezium_user;
GO

-- Verify the database user
USE AdventureWorksDW2022;
GO

SELECT
    name,
    type_desc
FROM sys.database_principals
WHERE name = 'debezium_user';

-- Test access as Debezium
USE AdventureWorksDW2022;
GO

EXECUTE AS USER = 'debezium_user';

SELECT TOP 5 *
FROM dbo.CDC_LearningCustomer;

CustomerID	CustomerName	City	Email	UpdatedAt
1	Amit Sharma	Pune	amit@gmail.com	2026-11-03 14:10:27.1458941
5	Sneha Roy	Chennai	sneha@gmail.com	2026-11-02 21:44:48.0470093
6	Arjun Das	Kolkata	arjun@gmail.com	2026-11-03 14:10:27.1448934

SELECT TOP 5 *
FROM cdc.dbo_CDC_LearningCustomer_CT;

__$start_lsn	__$end_lsn	__$seqval	__$operation	__$update_mask	CustomerID	CustomerName	City	Email	UpdatedAt	__$command_id
0x0000002B000059B80003	NULL	0x0000002B000059B80002	3	0x14	1	Amit Sharma	Hyderabad	amit@gmail.com	2026-11-02 21:44:48.0470093	1
0x0000002B000059B80003	NULL	0x0000002B000059B80002	4	0x14	1	Amit Sharma	Bangalore	amit@gmail.com	2026-11-03 13:12:40.1621875	1
0x0000002B00006F700003	NULL	0x0000002B00006F700002	2	0x1F	6	Arjun Das	Kolkata	arjun@gmail.com	2026-11-03 14:10:27.1448934	1
0x0000002B00006F780003	NULL	0x0000002B00006F780002	3	0x14	1	Amit Sharma	Bangalore	amit@gmail.com	2026-11-03 13:12:40.1621875	1
0x0000002B00006F780003	NULL	0x0000002B00006F780002	4	0x14	1	Amit Sharma	Pune	amit@gmail.com	2026-11-03 14:10:27.1458941	1

REVERT;
GO

USE AdventureWorksDW2022;
GO

EXECUTE AS USER = 'debezium_user';

SELECT TOP 5 *
FROM dbo.CDC_LearningCustomer;

SELECT TOP 5 *
FROM cdc.dbo_CDC_LearningCustomer_CT;

REVERT;
GO

select USER;

EXECUTE AS USER = 'dbo';


-----------------------------------------------------------------------------------------------------------------------

-- STEP 1: INSERT
INSERT INTO dbo.CDC_LearningCustomer
(
    CustomerID,
    CustomerName,
    City,
    Email,
    UpdatedAt
)
VALUES
(
    10,
    'Rahul Das',
    'Kolkata',
    'rahul@gmail.com',
    SYSUTCDATETIME()
);

-- Allow SQL Server CDC capture job to process the insert
WAITFOR DELAY '00:00:05';


-- STEP 2: UPDATE
UPDATE dbo.CDC_LearningCustomer
SET
    City = 'Bangalore',
    UpdatedAt = SYSUTCDATETIME()
WHERE CustomerID = 10;

-- Allow CDC to process the update
WAITFOR DELAY '00:00:05';


-- STEP 3: DELETE
DELETE FROM dbo.CDC_LearningCustomer
WHERE CustomerID = 10;

-- Allow CDC and Debezium to process the delete
WAITFOR DELAY '00:00:10';

SELECT
    SYSTEM_USER AS LoginName,
    USER_NAME() AS DatabaseUser,
    DB_NAME() AS DatabaseName;


USE AdventureWorksDW2022;
GO

SELECT
    SYSTEM_USER AS LoginName,
    USER_NAME() AS DatabaseUser,
    DB_NAME() AS DatabaseName;
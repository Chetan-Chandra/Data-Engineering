
-- #############################################  CDC AT SOURCE IMPLEMENTATION  ###############################################
-- ############################################################################################################################

-- Create our CDC learning table

USE AdventureWorksDW2022;
GO

-- Create a practice customer table
CREATE TABLE dbo.CDC_LearningCustomer
(
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    City VARCHAR(50),
    Email VARCHAR(100),
    UpdatedAt DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- Insert initial records
INSERT INTO dbo.CDC_LearningCustomer
    (CustomerID, CustomerName, City, Email)
VALUES
    (1, 'Amit Sharma', 'Kolkata', 'amit@gmail.com'),
    (2, 'Rahul Verma', 'Delhi', 'rahul@gmail.com'),
    (3, 'Priya Singh', 'Mumbai', 'priya@gmail.com');
GO

-- Verify records
SELECT * FROM dbo.CDC_LearningCustomer;
GO

-- ############################################################################################################################

-- Enable CDC at database level

USE AdventureWorksDW2022;
GO

EXEC sys.sp_cdc_enable_db;
GO

-- Verify
SELECT
    name,
    is_cdc_enabled
FROM sys.databases
WHERE name = 'AdventureWorksDW2022';
GO

/* OUTPUT:

	name					is_cdc_enabled
AdventureWorksDW2022			1

*/
-- ############################################################################################################################

-- Enable CDC on our customer table
USE AdventureWorksDW2022;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'CDC_LearningCustomer',
    @role_name = NULL,
    @supports_net_changes = 1;
GO
/* OUTPUT:
Update mask evaluation will be disabled in net_changes_function because the CLR configuration option is disabled.
Job 'cdc.AdventureWorksDW2022_capture' started successfully.
Job 'cdc.AdventureWorksDW2022_cleanup' started successfully.

Completion time: 2026-11-02T21:09:18.0339259+05:30
*/

-- Verify CDC is enabled for the table
SELECT
    name,
    is_tracked_by_cdc
FROM sys.tables
WHERE name = 'CDC_LearningCustomer';
GO

-- Inspect the CDC objects

USE AdventureWorksDW2022;
GO

-- Check CDC capture instances
EXEC sys.sp_cdc_help_change_data_capture;
GO

-- Check CDC change table
SELECT *
FROM cdc.dbo_CDC_LearningCustomer_CT;
GO

-- Check capture and cleanup jobs
EXEC sys.sp_cdc_help_jobs;
GO

-- Perform your first UPDATE
-- Now let's generate a change event.
-- Run this SQL:

USE AdventureWorksDW2022;
GO

SELECT * FROM dbo.CDC_LearningCustomer;

UPDATE dbo.CDC_LearningCustomer
SET
    City = 'Bengaluru',
    UpdatedAt = SYSDATETIME()
WHERE CustomerID = 1;
GO

SELECT *
FROM dbo.CDC_LearningCustomer
WHERE CustomerID = 1;

-- query the CDC table
SELECT *
FROM cdc.dbo_CDC_LearningCustomer_CT
ORDER BY __$start_lsn, __$seqval;

-- OUTPUT:
/*

__$start_lsn		__$end_lsn	__$seqval				__$operation	__$update_mask	CustomerID	CustomerName	City		Email			UpdatedAt						__$command_id
0x0000002A00000BD80003	NULL	0x0000002A00000BD80002		3				0x14			1		Amit Sharma		Kolkata		amit@gmail.com	2026-11-02 21:02:42.8947837			1
0x0000002A00000BD80003	NULL	0x0000002A00000BD80002		4				0x14			1		Amit Sharma		Bengaluru	amit@gmail.com	2026-11-02 21:16:25.6936468			1

SQL Server CDC normally records an UPDATE as two rows:
__$operation	Meaning	Expected City
3	Before update	Kolkata
4	After update	Bengaluru

Notice that both rows have the same LSN and sequence value. This connects the before and after images to the same update.

Column	Meaning
__$start_lsn	Transaction log sequence number
__$end_lsn	End LSN; commonly NULL in captured rows
__$seqval	Sequence value used to order changes
__$operation	Identifies INSERT, DELETE or UPDATE image
__$update_mask	Indicates which captured columns changed

Operation codes:
Code	Meaning
1	DELETE
2	INSERT
3	UPDATE — before image
4	UPDATE — after image

*/

-- Now test INSERT
USE AdventureWorksDW2022;
GO

INSERT INTO dbo.CDC_LearningCustomer
    (CustomerID, CustomerName, City, Email)
VALUES
    (4, 'Neha Das', 'Pune', 'neha@gmail.com');
GO

-- query the CDC table
SELECT *
FROM cdc.dbo_CDC_LearningCustomer_CT
ORDER BY __$start_lsn, __$seqval, __$operation;

/*

__$start_lsn			__$end_lsn	__$seqval				__$operation	__$update_mask	CustomerID		CustomerName	City		Email			UpdatedAt					__$command_id
0x0000002A00000BD80003	NULL		0x0000002A00000BD80002		3				0x14			1			Amit Sharma		Kolkata		amit@gmail.com	2026-11-02 21:02:42.8947837			1
0x0000002A00000BD80003	NULL		0x0000002A00000BD80002		4				0x14			1			Amit Sharma		Bengaluru	amit@gmail.com	2026-11-02 21:16:25.6936468			1
0x0000002A00000F280003	NULL		0x0000002A00000F280002		2				0x1F			4			Neha Das		Pune		neha@gmail.com	2026-11-02 21:25:04.0691705			1

*/

-- Now test DELETE
DELETE FROM dbo.CDC_LearningCustomer
WHERE CustomerID = 2;

-- query the CDC table
SELECT *
FROM cdc.dbo_CDC_LearningCustomer_CT
ORDER BY __$start_lsn, __$seqval, __$operation;
/*

__$start_lsn			__$end_lsn	__$seqval				__$operation	__$update_mask	CustomerID	CustomerName	City		Email				UpdatedAt						__$command_id
0x0000002A00000BD80003		NULL	0x0000002A00000BD80002		3				0x14			1		Amit Sharma		Kolkata		amit@gmail.com		2026-11-02 21:02:42.8947837			1
0x0000002A00000BD80003		NULL	0x0000002A00000BD80002		4				0x14			1		Amit Sharma		Bengaluru	amit@gmail.com		2026-11-02 21:16:25.6936468			1
0x0000002A00000F280003		NULL	0x0000002A00000F280002		2				0x1F			4		Neha Das		Pune		neha@gmail.com		2026-11-02 21:25:04.0691705			1
0x0000002A000010D00005		NULL	0x0000002A000010D00002		1				0x1F			2		Rahul Verma		Delhi		rahul@gmail.com		2026-11-02 21:02:42.8947837			1

*/


/*

Understand what happens in a real data pipeline
Imagine you have two tables:
- Source: CDC_LearningCustomer
- Target: TargetCustomer
Your source changes, and your pipeline needs to keep the target synchronised.

The processing rules are:
CDC operation	Target action
1 – DELETE	Delete the matching target record
2 – INSERT	Insert a new target record
3 – UPDATE before image	Ignore for current-state processing
4 – UPDATE after image	Update the matching target record

This is the foundation of a CDC-based incremental pipeline.
*/

 -- Create your target table
 -- For now, we'll create an empty target table. In the next step, we'll populate it with the initial source data and then apply CDC changes.
 -- ctrl+k, ctrl+c
 -- ctrl+k, ctrl+u

 USE AdventureWorksDW2022;
GO

CREATE TABLE dbo.TargetCustomer
(
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(100),
    City VARCHAR(50),
    Email VARCHAR(100),
    UpdatedAt DATETIME2
);
GO

SELECT * FROM dbo.TargetCustomer;

-- Now we'll perform the initial load into your target table before processing incremental CDC events.

-- This is how a typical CDC pipeline works:
-- 1. Initial load: Copy the existing source records into the target.
-- 2. Incremental load: Apply only subsequent INSERT, UPDATE and DELETE events captured by CDC.
-- Step 10 — Perform the initial load

USE AdventureWorksDW2022;
GO

INSERT INTO dbo.TargetCustomer
(
    CustomerID,
    CustomerName,
    City,
    Email,
    UpdatedAt
)
SELECT
    CustomerID,
    CustomerName,
    City,
    Email,
    UpdatedAt
FROM dbo.CDC_LearningCustomer;
GO

SELECT * 
FROM dbo.TargetCustomer
ORDER BY CustomerID;

-- CustomerID	CustomerName	City		Email				UpdatedAt
--	1			Amit Sharma		Bengaluru	amit@gmail.com		2026-11-02 21:16:25.6936468
--	3			Priya Singh		Mumbai		priya@gmail.com		2026-11-02 21:02:42.8947837
--	4			Neha Das		Pune		neha@gmail.com		2026-11-02 21:25:04.0691705

--Simulate new source changes
--Now we'll make three new changes after the initial load. This will simulate a real incremental pipeline.

USE AdventureWorksDW2022;
GO

-- 1. Update an existing customer
UPDATE dbo.CDC_LearningCustomer
SET
    City = 'Hyderabad',
    UpdatedAt = SYSDATETIME()
WHERE CustomerID = 1;

-- 2. Insert a new customer
INSERT INTO dbo.CDC_LearningCustomer
    (CustomerID, CustomerName, City, Email)
VALUES
    (5, 'Sneha Roy', 'Chennai', 'sneha@gmail.com');

-- 3. Delete an existing customer
DELETE FROM dbo.CDC_LearningCustomer
WHERE CustomerID = 3;
GO

--	What should happen?
--	CustomerID	Operation	Expected target action
--		1		UPDATE		Change Bengaluru to Hyderabad
--		5		INSERT		Add Sneha Roy
--		3		DELETE		Remove Priya Singh

-- -- query the CDC table
SELECT
    __$start_lsn,
    __$seqval,
    __$operation,
    CustomerID,
    CustomerName,
    City
FROM cdc.dbo_CDC_LearningCustomer_CT
ORDER BY __$start_lsn, __$seqval, __$operation;

--__$start_lsn				__$seqval				__$operation	CustomerID	CustomerName	City
--  0x0000002A00000BD80003	0x0000002A00000BD80002		3				1		Amit Sharma		Kolkata
-- 0x0000002A00000BD80003	0x0000002A00000BD80002		4				1		Amit Sharma		Bengaluru
-- 0x0000002A00000F280003	0x0000002A00000F280002		2				4		Neha Das		Pune
-- 0x0000002A000010D00005	0x0000002A000010D00002		1				2		Rahul Verma		Delhi
-- 0x0000002B000005880003	0x0000002B000005880002		3				1		Amit Sharma		Bengaluru
-- 0x0000002B000005880003	0x0000002B000005880002		4				1		Amit Sharma		Hyderabad
-- 0x0000002B000005900003	0x0000002B000005900002		2				5		Sneha Roy		Chennai
-- 0x0000002B000005980005	0x0000002B000005980002		1				3		Priya Singh		Mumbai

SELECT * FROM dbo.CDC_LearningCustomer;
--	CustomerID	CustomerName	City		Email				UpdatedAt
--		1		Amit Sharma		Hyderabad	amit@gmail.com		2026-11-02 21:44:48.0470093
--		4		Neha Das		Pune		neha@gmail.com		2026-11-02 21:25:04.0691705
--		5		Sneha Roy		Chennai		sneha@gmail.com		2026-11-02 21:44:48.0470093

SELECT * FROM dbo.TargetCustomer;
--	CustomerID	CustomerName	City		Email				UpdatedAt
--		1		Amit Sharma		Bengaluru	amit@gmail.com		2026-11-02 21:16:25.6936468
--		3		Priya Singh		Mumbai		priya@gmail.com		2026-11-02 21:02:42.8947837
--		4		Neha Das		Pune		neha@gmail.com		2026-11-02 21:25:04.0691705

-- Apply CDC changes using SQL
-- We'll use two operations:
-- 1. MERGE to insert and update records.
-- 2. DELETE to remove deleted records.

USE AdventureWorksDW2022;
GO

DECLARE @from_lsn BINARY(10) = 0x0000002B000005880003;

DECLARE @to_lsn BINARY(10) = sys.fn_cdc_get_max_lsn();

-- Step 1: Get the latest event per customer
;WITH LatestChanges AS
(
    SELECT
        CustomerID,
        CustomerName,
        City,
        Email,
        UpdatedAt,
        __$operation,
        ROW_NUMBER() OVER
        (
            PARTITION BY CustomerID
            ORDER BY __$start_lsn DESC,
                     __$seqval DESC,
                     __$operation DESC
        ) AS rn
    FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
    (
        @from_lsn,
        @to_lsn,
        'all'
    )
)

-- Step 2: Update or insert records
MERGE dbo.TargetCustomer AS T
USING
(
    SELECT *
    FROM LatestChanges
    WHERE rn = 1
      AND __$operation IN (2, 4)
) AS S
ON T.CustomerID = S.CustomerID

WHEN MATCHED THEN
    UPDATE SET
        T.CustomerName = S.CustomerName,
        T.City = S.City,
        T.Email = S.Email,
        T.UpdatedAt = S.UpdatedAt

WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        CustomerID,
        CustomerName,
        City,
        Email,
        UpdatedAt
    )
    VALUES
    (
        S.CustomerID,
        S.CustomerName,
        S.City,
        S.Email,
        S.UpdatedAt
    );

-- Step 3: Delete records
;WITH LatestChanges AS
(
    SELECT
        CustomerID,
        __$operation,
        ROW_NUMBER() OVER
        (
            PARTITION BY CustomerID
            ORDER BY __$start_lsn DESC,
                     __$seqval DESC,
                     __$operation DESC
        ) AS rn
    FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
    (
        @from_lsn,
        @to_lsn,
        'all'
    )
)
DELETE T
FROM dbo.TargetCustomer AS T
INNER JOIN LatestChanges AS S
    ON T.CustomerID = S.CustomerID
WHERE S.rn = 1
  AND S.__$operation = 1;
GO

SELECT * FROM dbo.CDC_LearningCustomer;
--	CustomerID	CustomerName	City		Email				UpdatedAt
--		1		Amit Sharma		Hyderabad	amit@gmail.com		2026-11-02 21:44:48.0470093
--		4		Neha Das		Pune		neha@gmail.com		2026-11-02 21:25:04.0691705
--		5		Sneha Roy		Chennai		sneha@gmail.com		2026-11-02 21:44:48.0470093

-- Verify your target table
SELECT *
FROM dbo.TargetCustomer
ORDER BY CustomerID;

--	CustomerID	CustomerName	City		Email				UpdatedAt
--		1		Amit Sharma		Hyderabad	amit@gmail.com		2026-11-02 21:44:48.0470093
--		4		Neha Das		Pune		neha@gmail.com		2026-11-02 21:25:04.0691705
--		5		Sneha Roy		Chennai		sneha@gmail.com		2026-11-02 21:44:48.0470093

USE AdventureWorksDW2022;
GO

SELECT
    SCHEMA_NAME(schema_id) AS SchemaName,
    name AS FunctionName,
    type_desc
FROM sys.objects
WHERE schema_id = SCHEMA_ID('cdc')
  AND type IN ('IF', 'TF')
ORDER BY name;

-- SchemaName		FunctionName										type_desc
--	 cdc			fn_cdc_get_all_changes_ ... 						SQL_INLINE_TABLE_VALUED_FUNCTION
--	 cdc			fn_cdc_get_all_changes_...							SQL_INLINE_TABLE_VALUED_FUNCTION
--	 cdc			fn_cdc_get_all_changes_dbo_CDC_LearningCustomer		SQL_INLINE_TABLE_VALUED_FUNCTION
--	 cdc			fn_cdc_get_net_changes_ ... 						SQL_INLINE_TABLE_VALUED_FUNCTION
--	 cdc			fn_cdc_get_net_changes_...							SQL_INLINE_TABLE_VALUED_FUNCTION
--   cdc			fn_cdc_get_net_changes_dbo_CDC_LearningCustomer		SQL_INLINE_TABLE_VALUED_FUNCTION

SELECT
    sys.fn_cdc_get_min_lsn('dbo_CDC_LearningCustomer')
        AS MinimumLSN,
    sys.fn_cdc_get_max_lsn()
        AS MaximumLSN;

-- MinimumLSN				MaximumLSN
-- 0x0000002A000000200007	0x0000002B000014E80001

USE AdventureWorksDW2022;
GO

EXEC sys.sp_cdc_help_change_data_capture;

--source_schema	source_table	capture_instance	object_id	source_object_id	start_lsn	end_lsn	supports_net_changes	has_drop_pending	role_name	index_name	filegroup_name	create_date	index_column_list	captured_column_list
--dbo	CDC_LearningCustomer	dbo_CDC_LearningCustomer	2018106230	1618104805	0x0000002A000000200007	NULL	1	NULL	NULL	PK__CDC_Lear__A4AE64B8DCC90A41	NULL	2026-11-02 21:09:11.500	[CustomerID]	[CustomerID], [CustomerName], [City], [Email], [UpdatedAt]

SELECT
    source_schema,
    source_table,
    capture_instance
FROM cdc.change_tables;


--__________________________________________________________________________________________________________________________________________________________________________

--##########################################################################################################################################################################
--__________________________________________________________________________________________________________________________________________________________________________

-- Create a checkpoint table
USE AdventureWorksDW2022;
GO

CREATE TABLE dbo.CDC_Checkpoint
(
    CaptureInstance VARCHAR(100) PRIMARY KEY,
    LastProcessedLSN BINARY(10),
    UpdatedAt DATETIME2 DEFAULT SYSDATETIME()
);

INSERT INTO dbo.CDC_Checkpoint
    (CaptureInstance, LastProcessedLSN)
VALUES
    ('dbo_CDC_LearningCustomer', 0x0000002B000014E80001);


SELECT * FROM dbo.CDC_Checkpoint;

-- CaptureInstance					LastProcessedLSN		UpdatedAt
-- dbo_CDC_LearningCustomer	    0x0000002B000014E80001	 2026-11-03 12:30:50.9350115
/*
Your checkpoint table is created, and the last processed LSN is saved:

0x0000002B000014E80001

Now we'll use this checkpoint to determine where the next CDC extraction should begin.

Step 2 — Calculate the next extraction range

SQL Server CDC provides a function called:
sys.fn_cdc_increment_lsn()
It returns the next valid LSN after the one supplied.

Run this query:
Understand the three values:
	Column				Meaning
LastProcessedLSN	Last successfully processed LSN
NextFromLSN			Starting point for the next extraction
CurrentMaxLSN		Latest LSN captured by SQL Server CDC
*/

DECLARE @LastProcessedLSN BINARY(10);

SELECT @LastProcessedLSN = LastProcessedLSN
FROM dbo.CDC_Checkpoint
WHERE CaptureInstance = 'dbo_CDC_LearningCustomer';

SELECT
    @LastProcessedLSN AS LastProcessedLSN,
    sys.fn_cdc_increment_lsn(@LastProcessedLSN) AS NextFromLSN,
    sys.fn_cdc_get_max_lsn() AS CurrentMaxLSN;

--	   LastProcessedLSN			 NextFromLSN				CurrentMaxLSN
--  0x0000002B000014E80001	  0x0000002B000014E80002	  0x0000002B00004BA00001

-- Notice that CurrentMaxLSN is greater than LastProcessedLSN. This means SQL Server CDC has captured additional changes since your previous batch.

-- Check whether new CDC records exist
-- Before applying anything to the target, let's inspect the new events.

-- Run this query:

DECLARE @FromLSN BINARY(10);
DECLARE @ToLSN BINARY(10);

SELECT @FromLSN =
    sys.fn_cdc_increment_lsn(LastProcessedLSN)
FROM dbo.CDC_Checkpoint
WHERE CaptureInstance = 'dbo_CDC_LearningCustomer';

SET @ToLSN = sys.fn_cdc_get_max_lsn();

SELECT *
FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
(
    @FromLSN,
    @ToLSN,
    'all'
)
ORDER BY __$start_lsn, __$seqval, __$operation;

-- __$start_lsn	 __$seqval	__$operation	__$update_mask	CustomerID	CustomerName	City	Email	UpdatedAt
/*
What this query does
- @FromLSN: Starts immediately after your saved checkpoint.
- @ToLSN: Captures the latest available CDC position.
- all: Returns both update before-images and after-images.
- ORDER BY: Displays changes in CDC sequence.
Important: We haven't intentionally created any new source changes after the previous batch. So the result might be empty. That's perfectly fine.
*/

--It means there are currently no new CDC changes after your saved checkpoint.

--Let's generate a new change and test whether our incremental extraction detects it.
--Step 4 — Generate a new source change
--Run this SQL statement:

select * from dbo.CDC_LearningCustomer;
-- CustomerID	CustomerName	  City		   Email					UpdatedAt
--	1			Amit Sharma		Hyderabad	amit@gmail.com		2026-11-02 21:44:48.0470093
--	4			Neha Das		Pune		neha@gmail.com		2026-11-02 21:25:04.0691705
--	5			Sneha Roy		Chennai		sneha@gmail.com		2026-11-02 21:44:48.0470093

USE AdventureWorksDW2022;
GO

UPDATE dbo.CDC_LearningCustomer
SET
    City = 'Bangalore',
    UpdatedAt = SYSDATETIME()
WHERE CustomerID = 1;

select * from dbo.CDC_LearningCustomer;
-- CustomerID	CustomerName	  City		   Email					UpdatedAt
--	1			Amit Sharma		Bangalore	amit@gmail.com		2026-11-03 13:12:40.1621875
--	4			Neha Das		Pune		neha@gmail.com		2026-11-02 21:25:04.0691705
--	5			Sneha Roy		Chennai		sneha@gmail.com		2026-11-02 21:44:48.0470093

-- check new CDC records:
DECLARE @FromLSN BINARY(10);
DECLARE @ToLSN BINARY(10);

SELECT @FromLSN =
    sys.fn_cdc_increment_lsn(LastProcessedLSN)
FROM dbo.CDC_Checkpoint
WHERE CaptureInstance = 'dbo_CDC_LearningCustomer';

SET @ToLSN = sys.fn_cdc_get_max_lsn();

SELECT *
FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
(
    @FromLSN,
    @ToLSN,
    'all'
)
ORDER BY __$start_lsn, __$seqval, __$operation;
/*
	__$start_lsn			__$seqval			__$operation	__$update_mask	CustomerID		CustomerName	City			Email					UpdatedAt
0x0000002B000059B80003	0x0000002B000059B80002		 4				0x14			1			Amit Sharma		Bangalore	 amit@gmail.com			2026-11-03 13:12:40.1621875
*/

-- Inspect the raw CDC change table
SELECT
    __$start_lsn,
    __$seqval,
    __$operation,
    CustomerID,
    CustomerName,
    City
FROM cdc.dbo_CDC_LearningCustomer_CT
WHERE CustomerID = 1
ORDER BY __$start_lsn DESC, __$seqval DESC;
/*
	__$start_lsn			__$seqval			__$operation	CustomerID		CustomerName	City
0x0000002B000059B80003	0x0000002B000059B80002		3				1			Amit Sharma		Hyderabad
0x0000002B000059B80003	0x0000002B000059B80002		4				1			Amit Sharma		Bangalore
0x0000002B000005880003	0x0000002B000005880002		3				1			Amit Sharma		Bengaluru
0x0000002B000005880003	0x0000002B000005880002		4				1			Amit Sharma		Hyderabad
0x0000002A00000BD80003	0x0000002A00000BD80002		3				1			Amit Sharma		Kolkata
0x0000002A00000BD80003	0x0000002A00000BD80002		4				1			Amit Sharma		Bengaluru
*/

DECLARE @FromLSN BINARY(10);
DECLARE @ToLSN BINARY(10);

SELECT @FromLSN =
    sys.fn_cdc_increment_lsn(LastProcessedLSN)
FROM dbo.CDC_Checkpoint
WHERE CaptureInstance = 'dbo_CDC_LearningCustomer';

SET @ToLSN = sys.fn_cdc_get_max_lsn();

SELECT
    @FromLSN AS FromLSN,
    @ToLSN AS ToLSN;

SELECT
    __$start_lsn,
    __$seqval,
    __$operation,
    CustomerID,
    City
FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
(
    @FromLSN,
    @ToLSN,
    'all'
);
/*
	FromLSN					ToLSN
0x0000002B000014E80002	0x0000002B00005BA80001

	__$start_lsn				__$seqval			__$operation	CustomerID		City
0x0000002B000059B80003	0x0000002B000059B80002			4				1			Bangalore
*/

-- Compare the two CDC function modes

DECLARE @UpdateLSN BINARY(10) = 0x0000002B000059B80003;

SELECT
    __$start_lsn,
    __$seqval,
    __$operation,
    CustomerID,
    City
FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
(
    @UpdateLSN,
    @UpdateLSN,
    'all'
)
ORDER BY __$operation;
/*
	__$start_lsn			__$seqval				__$operation	CustomerID		City
0x0000002B000059B80003	0x0000002B000059B80002			4				1			Bangalore
*/

-- Inspect the generated CDC function

EXEC sys.sp_cdc_help_change_data_capture
    @source_schema = N'dbo',
    @source_name = N'CDC_LearningCustomer';
/*
source_schema	source_table	capture_instance	object_id	source_object_id	start_lsn	end_lsn	supports_net_changes	has_drop_pending	role_name	index_name	filegroup_name	create_date	index_column_list	captured_column_list
dbo	CDC_LearningCustomer	dbo_CDC_LearningCustomer	2018106230	1618104805	0x0000002A000000200007	NULL	1	NULL	NULL	PK__CDC_Lear__A4AE64B8DCC90A41	NULL	2026-11-02 21:09:11.500	[CustomerID]	[CustomerID], [CustomerName], [City], [Email], [UpdatedAt]
*/

SELECT OBJECT_DEFINITION(
    OBJECT_ID('cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer')
) AS FunctionDefinition;
/*

FunctionDefinition
   create function [cdc].[fn_cdc_get_all_changes_dbo_CDC_LearningCustomer]   ( @from_lsn binary(10),    @to_lsn binary(10),    @row_filter_option nvarchar(30)   )   returns table   return      select NULL as __$start_lsn,    NULL as __$seqval,    NULL as __$operation,    NULL as __$update_mask, NULL as [CustomerID], NULL as [CustomerName], NULL as [City], NULL as [Email], NULL as [UpdatedAt]   where ( [sys].[fn_cdc_check_parameters]( N'dbo_CDC_LearningCustomer', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 0)     union all      select t.__$start_lsn as __$start_lsn,    t.__$seqval as __$seqval,    t.__$operation as __$operation,    t.__$update_mask as __$update_mask, t.[CustomerID], t.[CustomerName], t.[City], t.[Email], t.[UpdatedAt]   from [cdc].[dbo_CDC_LearningCustomer_CT] t with (nolock)       where (lower(rtrim(ltrim(@row_filter_option))) = 'all')       and ( [sys].[fn_cdc_check_parameters]( N'dbo_CDC_LearningCustomer', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)    and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4)    and (t.__$start_lsn <= @to_lsn)    and (t.__$start_lsn >= @from_lsn)       union all        select t.__$start_lsn as __$start_lsn,    t.__$seqval as __$seqval,    t.__$operation as __$operation,    t.__$update_mask as __$update_mask, t.[CustomerID], t.[CustomerName], t.[City], t.[Email], t.[UpdatedAt]   from [cdc].[dbo_CDC_LearningCustomer_CT] t with (nolock)        where (lower(rtrim(ltrim(@row_filter_option))) = 'all update old')       and ( [sys].[fn_cdc_check_parameters]( N'dbo_CDC_LearningCustomer', @from_lsn, @to_lsn, lower(rtrim(ltrim(@row_filter_option))), 0) = 1)    and (t.__$operation = 1 or t.__$operation = 2 or t.__$operation = 4 or         t.__$operation = 3 )    and (t.__$start_lsn <= @to_lsn)    and (t.__$start_lsn >= @from_lsn)   

*/

-- Test the correct option

DECLARE @UpdateLSN BINARY(10) = 0x0000002B000059B80003;

SELECT
    __$start_lsn,
    __$seqval,
    __$operation,
    CustomerID,
    City
FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
(
    @UpdateLSN,
    @UpdateLSN,
    'all update old'
)
ORDER BY __$operation;
/*
	__$start_lsn			__$seqval			__$operation	CustomerID		 City
0x0000002B000059B80003	0x0000002B000059B80002		3				1			Hyderabad
0x0000002B000059B80003	0x0000002B000059B80002		4				1			Bangalore
*/

-- Extract new CDC events using the CDC function
DECLARE @FromLSN BINARY(10);
DECLARE @ToLSN BINARY(10);

SELECT @FromLSN =
    sys.fn_cdc_increment_lsn(LastProcessedLSN)
FROM dbo.CDC_Checkpoint
WHERE CaptureInstance = 'dbo_CDC_LearningCustomer';

SET @ToLSN = sys.fn_cdc_get_max_lsn();

SELECT *
FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
(
    @FromLSN,
    @ToLSN,
    'all'
);

--		__$start_lsn				__$seqval			__$operation	__$update_mask	CustomerID		CustomerName	City			Email					UpdatedAt
-- 0x0000002B000059B80003	0x0000002B000059B80002			4					0x14		1			Amit Sharma		Bangalore	amit@gmail.com		2026-11-03 13:12:40.1621875

-- Apply the extracted CDC events using MERGE to TargetCustomer

DECLARE @FromLSN BINARY(10);
DECLARE @ToLSN BINARY(10);

SELECT @FromLSN =
    sys.fn_cdc_increment_lsn(LastProcessedLSN)
FROM dbo.CDC_Checkpoint
WHERE CaptureInstance = 'dbo_CDC_LearningCustomer';

SET @ToLSN = sys.fn_cdc_get_max_lsn();

;WITH CDC_Changes AS
(
    SELECT
        CustomerID,
        CustomerName,
        City,
        Email,
        UpdatedAt,
        ROW_NUMBER() OVER
        (
            PARTITION BY CustomerID
            ORDER BY __$start_lsn DESC, __$seqval DESC
        ) AS rn
    FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
    (
        @FromLSN,
        @ToLSN,
        'all'
    )
    WHERE __$operation IN (2, 4)
)
MERGE dbo.TargetCustomer AS T
USING
(
    SELECT *
    FROM CDC_Changes
    WHERE rn = 1
) AS S
ON T.CustomerID = S.CustomerID

WHEN MATCHED THEN
    UPDATE SET
        T.CustomerName = S.CustomerName,
        T.City = S.City,
        T.Email = S.Email,
        T.UpdatedAt = S.UpdatedAt

WHEN NOT MATCHED BY TARGET THEN
    INSERT (CustomerID, CustomerName, City, Email, UpdatedAt)
    VALUES (S.CustomerID, S.CustomerName, S.City, S.Email, S.UpdatedAt);
-- (1 row affected)

--Verify the target
SELECT *
FROM dbo.TargetCustomer
WHERE CustomerID = 1;

--	CustomerID		CustomerName	City			Email				UpdatedAt
--		1			Amit Sharma		Bangalore		amit@gmail.com		2026-11-03 13:12:40.1621875

-- Update the checkpoint
DECLARE @ToLSN BINARY(10);

SET @ToLSN = sys.fn_cdc_get_max_lsn();

UPDATE dbo.CDC_Checkpoint
SET
    LastProcessedLSN = @ToLSN,
    UpdatedAt = SYSDATETIME()
WHERE CaptureInstance = 'dbo_CDC_LearningCustomer';

SELECT *
FROM dbo.CDC_Checkpoint;

--		CaptureInstance				LastProcessedLSN		UpdatedAt
--	dbo_CDC_LearningCustomer	0x0000002B00006CA80001	2026-11-03 14:04:05.6045834

-- Generate three new changes
-- 1. INSERT
INSERT INTO dbo.CDC_LearningCustomer
    (CustomerID, CustomerName, City, Email)
VALUES
    (6, 'Arjun Das', 'Kolkata', 'arjun@gmail.com');

-- 2. UPDATE
UPDATE dbo.CDC_LearningCustomer
SET
    City = 'Pune',
    UpdatedAt = SYSDATETIME()
WHERE CustomerID = 1;

-- 3. DELETE
DELETE FROM dbo.CDC_LearningCustomer
WHERE CustomerID = 4;

-- Extract new CDC events using the CDC function
DECLARE @FromLSN BINARY(10);
DECLARE @ToLSN BINARY(10);

SELECT @FromLSN =
    sys.fn_cdc_increment_lsn(LastProcessedLSN)
FROM dbo.CDC_Checkpoint
WHERE CaptureInstance = 'dbo_CDC_LearningCustomer';

SET @ToLSN = sys.fn_cdc_get_max_lsn();

SELECT *
FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
(
    @FromLSN,
    @ToLSN,
    'all'
);

--			__$start_lsn			__$seqval		__$operation	__$update_mask	CustomerID		CustomerName	City			Email				UpdatedAt
--	0x0000002B00006F700003	0x0000002B00006F700002		2				0x1F			6			Arjun Das		Kolkata		arjun@gmail.com		2026-11-03 14:10:27.1448934
--	0x0000002B00006F780003	0x0000002B00006F780002		4				0x14			1			Amit Sharma		Pune		amit@gmail.com		2026-11-03 14:10:27.1458941
--	0x0000002B00006F800005	0x0000002B00006F800002		1				0x1F			4			Neha Das		Pune		neha@gmail.com		2026-11-02 21:25:04.0691705

-- Apply INSERT, UPDATE and DELETE

DECLARE @FromLSN BINARY(10);
DECLARE @ToLSN BINARY(10);

SELECT @FromLSN =
    sys.fn_cdc_increment_lsn(LastProcessedLSN)
FROM dbo.CDC_Checkpoint
WHERE CaptureInstance = 'dbo_CDC_LearningCustomer';

SET @ToLSN = sys.fn_cdc_get_max_lsn();

-- Extract the batch
SELECT *
INTO #CDC_Changes
FROM cdc.fn_cdc_get_all_changes_dbo_CDC_LearningCustomer
(
    @FromLSN,
    @ToLSN,
    'all'
);

BEGIN TRANSACTION;

-- 1. Apply INSERT and UPDATE
;WITH LatestChanges AS
(
    SELECT *,
        ROW_NUMBER() OVER
        (
            PARTITION BY CustomerID
            ORDER BY __$start_lsn DESC,
                     __$seqval DESC
        ) AS rn
    FROM #CDC_Changes
    WHERE __$operation IN (2, 4)
)
MERGE dbo.TargetCustomer AS T
USING
(
    SELECT *
    FROM LatestChanges
    WHERE rn = 1
) AS S
ON T.CustomerID = S.CustomerID

WHEN MATCHED THEN
    UPDATE SET
        T.CustomerName = S.CustomerName,
        T.City = S.City,
        T.Email = S.Email,
        T.UpdatedAt = S.UpdatedAt

WHEN NOT MATCHED BY TARGET THEN
    INSERT
    (
        CustomerID, CustomerName, City, Email, UpdatedAt
    )
    VALUES
    (
        S.CustomerID, S.CustomerName, S.City,
        S.Email, S.UpdatedAt
    );

-- 2. Apply DELETE events
DELETE T
FROM dbo.TargetCustomer AS T
INNER JOIN
(
    SELECT DISTINCT CustomerID
    FROM #CDC_Changes
    WHERE __$operation = 1
) AS D
ON T.CustomerID = D.CustomerID;

-- 3. Update checkpoint
UPDATE dbo.CDC_Checkpoint
SET
    LastProcessedLSN = @ToLSN,
    UpdatedAt = SYSDATETIME()
WHERE CaptureInstance = 'dbo_CDC_LearningCustomer';

COMMIT TRANSACTION;

DROP TABLE #CDC_Changes;

-- Verify the target
SELECT *
FROM dbo.TargetCustomer
ORDER BY CustomerID;

--	CustomerID	CustomerName	City			Email				UpdatedAt
--		1		Amit Sharma		Pune		amit@gmail.com		2026-11-03 14:10:27.1458941
--		5		Sneha Roy		Chennai		sneha@gmail.com		2026-11-02 21:44:48.0470093
--		6		Arjun Das		Kolkata		arjun@gmail.com		2026-11-03 14:10:27.1448934
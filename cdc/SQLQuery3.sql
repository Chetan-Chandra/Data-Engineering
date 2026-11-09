SELECT
    SYSTEM_USER AS LoginName,
    USER_NAME() AS DatabaseUser,
    DB_NAME() AS DatabaseName;


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

LoginName	DatabaseUser	DatabaseName
CHETAN\Admin	dbo	AdventureWorksDW2022

USE AdventureWorksDW2022;
GO

SELECT *
FROM dbo.CDC_LearningCustomer
WHERE CustomerID = 10;

CustomerID	CustomerName	City	Email	UpdatedAt
-- To check SP 

USE [NEURON]
GO 

IF OBJECT_ID('RemoteStatus_INSERT','P') IS NOT NULL
BEGIN
	RAISERROR('SP Present!',0,0);
END

-- Rough codes for partition switching

USE [PartitionThis]
GO 

-- Checking if [dbo].[OrdersDaily] exists
IF OBJECT_ID('[dbo].[OrdersDaily]') IS NOT NULL
	BEGIN 
		RAISERROR('[dbo].[OrdersDaily] exists!',0,0);
	END 
ELSE
	BEGIN
		RAISERROR('[dbo].[OrdersDaily] doesn''t exist!',0,0);
	END 

-- Checking the object in sys.objects
SELECT COUNT(*) FROM sys.objects;
SELECT * FROM sys.objects;

-- dropping previous user defined tables
DROP TABLE IF EXISTS [dbo].[OrdersDaily];
DROP TABLE IF EXISTS [dbo].[OrdersDaily_Without_Partition];

-- checking partition function and partition schemes
SELECT * FROM sys.partition_functions;
SELECT * FROM sys.partition_schemes;

-- dropping previous partition scheme and partition function 
DROP PARTITION SCHEME DailyPS;
DROP PARTITION FUNCTION DailyPF;

-- checking out partition stats 
SELECT * FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('[dbo].[OrdersDaily]');

SELECT * FROM sys.partition_range_values
WHERE function_id = (SELECT TOP (1) function_id 
						FROM sys.partition_functions 
						WHERE name = 'DailyPF');

SELECT partition_number, rows
FROM sys.partitions
WHERE OBJECT_NAME(OBJECT_ID)='OrdersDaily';


-- truncating partition values
TRUNCATE TABLE [dbo].[OrdersDaily]
WITH (PARTITIONS (2 to 6));


SELECT * FROM OrdersDailyLoad;
SELECT * FROM OrdersDailyLoad2;

CREATE TABLE OrdersDailyLoad2 (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY (10001,1) NOT NULL,
	OrderName nvarchar(256) NOT NULL
) 
GO
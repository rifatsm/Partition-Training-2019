--******************
--Original from Brent Ozar PLF, LLC DBA Brent Ozar Unlimited and Kendra Little
--Modified by: Rifat Sabbir Mansur
--******************

--******************
--Don't just run the whole thing.
--Run this step by step to learn!
--******************

DECLARE @msg NVARCHAR(MAX);
SET @msg = N'This script is not meant to execute all at once!
Please highlight and execute each section individually.
Also, make sure to run it in a TEST ENVIRONMENT!'

RAISERROR(@msg,20,1) WITH LOG;
GO

--******************
--1. CREATE OUR DEMO DATABASE
--Blow it away if it already exists
--******************


IF db_id('PartitionThis') IS NOT NULL 
BEGIN
	USE master; 
	ALTER DATABASE [PartitionThis] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE [PartitionThis];
END 
GO

CREATE DATABASE [PartitionThis]
GO

ALTER DATABASE [PartitionThis]
	MODIFY FILE ( NAME = N'PartitionThis', SIZE = 256MB , MAXSIZE = 10GB , FILEGROWTH = 512MB );
ALTER DATABASE [PartitionThis]	
	MODIFY FILE ( NAME = N'PartitionThis_log', SIZE = 128MB , FILEGROWTH = 128MB );
GO

USE PartitionThis;
GO


--*******************************
--2 CREATE HELPER OBJECTS
--Why do we need these?
--Do they HAVE to be in the database with the partitioned objects?
--*******************************

--Create a schema for "partition helper" objects
CREATE SCHEMA [ph] AUTHORIZATION dbo;
GO

--Create a tally table with ~1 million rows, we'll use this to populate test tables
--This general method attributed to Itzik Ben-Gan
IF OBJECT_ID('ph.tally','U') IS NULL
BEGIN
	WITH    Pass0 AS ( SELECT   1 AS C UNION ALL SELECT   1), 
		Pass1 AS ( SELECT   1 AS C FROM     Pass0 AS A , Pass0 AS B),
		Pass2 AS ( SELECT   1 AS C FROM     Pass1 AS A , Pass1 AS B),
		Pass3 AS ( SELECT   1 AS C FROM     Pass2 AS A , Pass2 AS B),
		Pass4 AS ( SELECT   1 AS C FROM     Pass3 AS A , Pass3 AS B),
		Pass5 AS ( SELECT   1 AS C FROM     Pass4 AS A , Pass4 AS B),
		tally AS ( SELECT   row_number() OVER ( Order BY C ) AS N FROM Pass5 )
	SELECT  N
	INTO    ph.tally
	FROM    tally
	WHERE   N <= 1000000;
END
ELSE
BEGIN
	RAISERROR('ph.tally already exists',0,0);
END 
GO

-- View the top 1000 records from ph.tally
SELECT TOP (1000) * FROM ph.tally;


--******************
--3. CREATE OUR HERO, THE PARTITION FUNCTION
--Daily Example: RIGHT bound partition function 
--Cool point: It can use variables and functions
--******************

-- Drop existing Partiton Scheme and Partition Function 

-- SELECT * FROM sys.partition_functions;

-- DROP PARTITION FUNCTION DailyPF;

--Create the partition function: dailyPF
DECLARE @StartDay DATE='20190101';
CREATE PARTITION FUNCTION DailyPF (DATETIME2(0))
    AS RANGE RIGHT FOR VALUES
    (@StartDay, 
	DATEADD(dd,1,@StartDay), 
	DATEADD(dd,2,@StartDay),  
	DATEADD(dd,3,@StartDay), 
	DATEADD(dd,4,@StartDay) ); -- 5 values 
GO

--Here's how we see the partition function
SELECT name,type_desc, fanout, boundary_value_on_right, create_date 
FROM sys.partition_functions;
GO




--******************
--4. SET UP SOME FILEGROUPS and FILES FOR OUR PARTITIONS TO LIVE ON.
--In production they MIGHT be on different drives with the 
--appropriate RAID and spindles.
--******************

--Add filegroups.
--Number of filegroups = 1 + Number of boundary points 
--defined in partition function

ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG1
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG2
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG3
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG4
GO 
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG5
GO
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG6
GO

--SELECT TOP (1000) * FROM sys.database_files;


--Add files to the filegroups
--This is being done dynamically so it will work on different instances, 
--but it makes some big assumptions!
DECLARE @path NVARCHAR(256), @i TINYINT=1, @sql NVARCHAR(4000);
SELECT TOP 1 @path=LEFT(physical_name,LEN(physical_name)-4) 
FROM sys.database_files WHERE name='PartitionThis';

WHILE @i <= 6
BEGIN
	SET @sql=N'ALTER DATABASE PartitionThis ADD FILE (name=DailyF' + CAST(@i AS NCHAR(1))+', 
		 filename=''' +  @path + N'F'+ CAST(@i AS NCHAR(1))+'.ndf' + ''',
		 size=128MB, filegrowth=256MB) TO FILEGROUP DailyFG'+CAST(@i AS NCHAR(1))
	--show the command we're running
	RAISERROR (@sql,0,0)
	
	--run it
	EXEC sp_executesql @sql;
	SET @i+=1;
END
GO

--SELECT TOP (1000) * FROM sys.database_files;




--******************
--5. CREATE THE PARTITION SCHEME
--This maps the filegroups to the partition function.
--******************

SELECT * FROM sys.partition_schemes;

-- DROP PARTITION SCHEME DailyPS;

--Create the partition scheme: dailyPS 
CREATE PARTITION SCHEME DailyPS 
	AS PARTITION DailyPF
	TO (DailyFG1, DailyFG2, DailyFG3, DailyFG4, DailyFG5, DailyFG6);


-- SELECT * FROM sys.partition_functions;

--******************
--6. CREATE OBJECTS ON THE PARTITION SCHEME
--******************

--Create a partitioned heap... yep, you can do that!
--When would a partitioned heap be useful?
--What could go wrong with a partitioned heap?
if OBJECT_ID('OrdersDaily','U') is null
CREATE TABLE OrdersDaily (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on DailyPS(OrderDate)
GO

-- DROP TABLE OrdersDaily;

if OBJECT_ID('OrdersDaily_Without_Partition','U') is null
CREATE TABLE OrdersDaily_Without_Partition  (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int NOT NULL,
	OrderName nvarchar(256) NOT NULL
) 
GO


--******************
--6. INSERT ROWS
--******************

--Let's insert some rows!
--We are leaving Partition 1 and Partition 6 empty on purpose-- 
--it's a best practice to have empty partitions at each end.

-- date
DECLARE @date VARCHAR(10) = '20190101';
DECLARE @number_of_rows INT = 1000000;
INSERT INTO OrdersDaily (OrderDate, OrderId, OrderName)
SELECT DATEADD(ss, t.N % 86400, CAST(CAST(@date AS DATE) AS DATETIME2(0))) AS OrderDate,
t.N AS OrderId,
N'PGroup1' AS OrderName
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Sanity check:

--SELECT TOP (100) * FROM dbo.OrdersDaily;
--SELECT COUNT(*) FROM dbo.OrdersDaily
--WHERE date_time >= '20190716';

-- date + 1
DECLARE @date VARCHAR(10) = '20190101';
DECLARE @number_of_rows INT = 1000000;
INSERT INTO OrdersDaily (OrderDate, OrderId, OrderName)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,1,CAST(CAST(@date AS DATE) AS DATETIME2(0)))) AS OrderDate,
(t.N + (@number_of_rows * 1)) AS OrderId,
N'PGroup2' AS OrderName
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Sanity check:

--SELECT TOP (100) * FROM dbo.TEST_with_Partition;
--SELECT COUNT(*) FROM dbo.TEST_with_Partition
--WHERE date_time >= '20190715' AND date_time < '20190716';

-- date + 2 
DECLARE @date VARCHAR(10) = '20190101';
DECLARE @number_of_rows INT = 1000000;
DECLARE @days_to_add INT = 2;
INSERT INTO OrdersDaily (OrderDate, OrderId, OrderName)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,@days_to_add,CAST(CAST(@date AS DATE) AS DATETIME2(0)))) AS OrderDate,
(t.N + (@number_of_rows * @days_to_add)) AS OrderId,
N'PGroup3' AS OrderName
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO


-- Sanity check:

--SELECT TOP (100) * FROM dbo.TEST_with_Partition;
--SELECT COUNT(*) FROM dbo.TEST_with_Partition
--WHERE date_time >= '20190714' AND date_time < '20190715';

-- date + 3 
DECLARE @date VARCHAR(10) = '20190101';
DECLARE @number_of_rows INT = 1000000;
DECLARE @days_to_add INT = 3;
INSERT INTO OrdersDaily (OrderDate, OrderId, OrderName)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,@days_to_add,CAST(CAST(@date AS DATE) AS DATETIME2(0)))) AS OrderDate,
(t.N + (@number_of_rows * @days_to_add)) AS OrderId,
N'PGroup4' AS OrderName
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Sanity check:

--SELECT TOP (100) * FROM dbo.TEST_with_Partition;
--SELECT COUNT(*) FROM dbo.TEST_with_Partition
--WHERE date_time >= '20190713' AND date_time < '20190714';

-- date + 4 
DECLARE @date VARCHAR(10) = '20190101';
DECLARE @number_of_rows INT = 1000000;
DECLARE @days_to_add INT = 4;
INSERT INTO OrdersDaily (OrderDate, OrderId, OrderName)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,@days_to_add,CAST(CAST(@date AS DATE) AS DATETIME2(0)))) AS OrderDate,
(t.N + (@number_of_rows * @days_to_add)) AS OrderId,
N'PGroup5' AS OrderName
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO


-- Sanity check:

--SELECT TOP (100) * FROM dbo.OrdersDaily
--SELECT COUNT(*) FROM dbo.OrdersDaily
--WHERE OrderDate >= '20190105' AND OrderDate < '20190106';

-- SELECT COUNT(*) FROM dbo.OrdersDaily;
-- TRUNCATE TABLE dbo.OrdersDaily;

SELECT *
FROM ph.FileGroupDetail;
GO


--******************
--8. SWITCHING IN NEW PARTITIONS...
--Like lightning.
--******************

--I want to load data for another day and then switch it in.
--First, add a filegroup.
ALTER DATABASE PartitionThis ADD FILEGROUP DailyFG7

SELECT * FROM sys.filegroups;

--Add a file for the filegroup.
DECLARE @path NVARCHAR(256), @i TINYINT=7, @sql NVARCHAR(4000);
SELECT TOP 1 @path=LEFT(physical_name,LEN(physical_name)-4) 
FROM sys.database_files WHERE name='PartitionThis';
RAISERROR(N'The path is: %s',0,0, @path);

WHILE @i = 7
BEGIN
	SET @sql=N'ALTER DATABASE PartitionThis ADD FILE (name=DailyF' + CAST(@i AS NCHAR(1))+', 
		 filename=''' +  @path + N'F'+ CAST(@i AS NCHAR(1))+'.ndf' + ''',
		 size=128MB, filegrowth=256MB) TO FILEGROUP DailyFG'+CAST(@i AS NCHAR(1))
	--show the command we're running
	RAISERROR (@sql,0,0)
	
	--run it
	EXEC sp_executesql @sql
	SET @i+=1
END

--SELECT TOP (1000) * FROM sys.database_files;

--SELECT * FROM ph.FileGroupDetail;

--Create a staging table on our new filegroup (dailyFG6)
--Why are we seeding the identity here?
--What would happen if we didn't?
--Ans: https://dba.stackexchange.com/questions/183237/partition-switch-what-is-the-benefit-if-staging-has-to-be-same-as-destination/183243
--this reduces the amount of contention for locks/resource on the primary table
CREATE TABLE OrdersDailyLoad (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on [DailyFG7]
GO

--DROP TABLE OrdersDailyLoad

SELECT * FROM OrdersDailyLoad;

--Insert some records into our staging table
-- date + 5 
DECLARE @date VARCHAR(10) = '20190101';
DECLARE @number_of_rows INT = 1000000;
DECLARE @days_to_add INT = 5;
INSERT INTO OrdersDailyLoad (OrderDate, OrderId, OrderName)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,@days_to_add,CAST(CAST(@date AS DATE) AS DATETIME2(0)))) AS OrderDate,
(t.N + (@number_of_rows * @days_to_add)) AS OrderId,
N'PGroup6' AS OrderName
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO


--Set our new filegroup as 'Next used' in our partition scheme
--This is how you add it to the partition scheme
ALTER PARTITION SCHEME DailyPS
NEXT USED DailyFG7

--This means DailyFG7 will receive any additional partition of a 
--partitioned table or index as a result of an ALTER PARTITION 
--FUNCTION statement.

--Examine our partition function with associated scheme, filegroups, 
--and boundary points
SELECT *
FROM ph.FileGroupDetail;
GO


--Add a new boundary point to our partition function. 
--We already have an empty partition -- there's no data right 
--now in Partition #6.
--But we always want to KEEP at least one empty partition at the high end, 
--so we're going to add another.
DECLARE @date VARCHAR(10) = '20190101';
DECLARE @number_of_rows INT = 1000000;
DECLARE @days_to_add INT = 5;
ALTER PARTITION FUNCTION DailyPF() 
SPLIT RANGE (DATEADD(dd,@days_to_add,CAST(CAST(@date AS DATE) AS DATETIME2(0))))
GO

-- SELECT * FROM ph.FileGroupDetail;

--If you don't add a filegroup to the partition scheme first with NEXT USED, 
--you'll get the error:
--Msg 7707, Level 16, State 1, Line 2
--The associated partition function 'DailyPF' generates more partitions 
--than there are file groups mentioned in the scheme 'DailyPS'.

-- OR (in my case)
--Msg 7710, Level 16, State 1, Line 578
--Warning: The partition scheme 'DailyPS' does not have any next used filegroup. Partition scheme has not been changed.


--But note that you *CAN* use a FileGroup for  more than one partition
--To do this,  you just set an existing one with NEXT USED.



--Now check the partition function and object-- what's different?
-- Ans. The new partition has partition_number and range_value (previous NULL) 
SELECT *
FROM ph.FileGroupDetail

SELECT *
FROM ph.ObjectDetail
WHERE object_name IN ('OrdersDaily','OrdersDailyLoad')
ORDER BY object_name, partition_number



--******************
--Switch in!
--******************
ALTER TABLE OrdersDailyLoad
SWITCH TO OrdersDaily PARTITION 7
GO


-- Add constraints 

DECLARE @date VARCHAR(10) = '20190101';
DECLARE @tsql NVARCHAR(2000)=
'ALTER TABLE OrdersDailyLoad
WITH CHECK
ADD CONSTRAINT CKOrdersDailyLoad_LowEnd
CHECK (OrderDate >= ''' + 
	convert(CHAR(10),DATEADD(dd,5,CAST(CAST(@date AS DATE) AS DATETIME2(0)))) + ''')'
--Display what we're running
RAISERROR (@tsql,0,0)
--Run it
EXEC sp_executesql @tsql;
GO

DECLARE @date VARCHAR(10) = '20190101';
DECLARE @tsql NVARCHAR(2000)=
'ALTER TABLE OrdersDailyLoad
WITH CHECK
ADD CONSTRAINT CKOrdersDailyLoad_HighEnd
CHECK (OrderDate < ''' + 
	convert(CHAR(10),DATEADD(dd,6,CAST(CAST(@date AS DATE) AS DATETIME2(0)))) + ''')'
--Display what we're running
RAISERROR (@tsql,0,0)
--Run it
EXEC sp_executesql @tsql;

-- TRUNCATE TABLE OrdersDaily WITH (PARTITIONS (7));

-- Let's look at our partitioned table and loading table now...
-- Partition 6 should now have 5000 rows in it
-- Partition 1 and Partition 7 should be empty
SELECT *
FROM ph.ObjectDetail
WHERE object_name IN ('OrdersDaily','OrdersDailyLoad')
ORDER BY object_name, partition_number;
GO

--Let's go ahead and drop the staging table
DROP TABLE OrdersDailyLoad;
GO



--******************
--9. SWITCHING OUT OLD DATA
--******************
--I have four right partition boundaries currently
--I want to switch out my oldest data


--Look at how this is mapped out now. 
--We want to get rid of our oldest 1000 rows.
--Those are sitting in Partition 2 which is on DailyFG2
SELECT *
FROM ph.FileGroupDetail
WHERE pf_name = 'DailyPF'
ORDER BY partition_number;
GO


--Create a staging table to hold switched out data 
--PUT THIS ON THE SAME FILEGROUP YOU'RE SWITCHING OUT OF
CREATE TABLE OrdersDailyOut (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on [DailyFG2];
GO



--Switch OUT!
RAISERROR ('Switching out.',0,0)
ALTER TABLE OrdersDaily
SWITCH PARTITION 2 TO OrdersDailyOut;
GO

--Look at our switch OUT table
--OrdersDailyOut should have 1000 rows
--Partition 1 and Parttiion 2 of ORdersDaily should have 0 rows
SELECT *
FROM ph.ObjectDetail
WHERE object_name IN ('OrdersDaily','OrdersDailyOut')
ORDER BY object_name DESC, partition_number;
GO


--We want to keep an empty partition on DailyFG1
--But we want to remove the empty partition on DailyFG2 (currently Partition 2)
--Programmatically find the boundary point to merge
--This is done so we don't have to hard code dates in the script


DECLARE @MergeBoundaryPoint DATETIME2(0), @msg NVARCHAR(2000);
SELECT @MergeBoundaryPoint = CAST(MIN(range_val.value) AS DATETIME2(0))
FROM sys.partition_functions  part_func
JOIN sys.partition_range_values range_val ON part_func.function_id=range_val.function_id
where part_func.name='DailyPF'

PRINT(@MergeBoundaryPoint);

IF (
	SELECT COUNT(*)
	FROM dbo.OrdersDaily
	WHERE OrderDate < dateadd(DAY, 1, @MergeBoundaryPoint)
) =0
BEGIN
	SET @msg='No records found, merging boundary point ' 
		+ CAST(@MergeBoundaryPoint AS CHAR(10)) + '.'
	RAISERROR (@msg,0,0)
	ALTER PARTITION FUNCTION DailyPF ()
		MERGE RANGE ( @MergeBoundaryPoint )
END
ELSE
BEGIN
	SET @msg='ERROR: Records exist around boundary point ' 
		+ CAST(@MergeBoundaryPoint AS CHAR(10)) + '. Not merging.'
	RAISERROR (@msg,16,1)
END

--Homework: What would happen if we didn't have a 
--safety to make sure there were no records?


--Look at how this is mapped out after switch-out and merging boundary points.
--DailyFG1 should be present and have 0 rows.
--No partitions should be mapped to DailyFG2 --
--our boundary point merge got rid of it. (It was empty.)
SELECT *
FROM ph.FileGroupDetail;
GO


--Let's go ahead and drop the switch OUT table
--(Assuming we don't want to do anything with the rows we switched out!)
DROP TABLE OrdersDailyOut;
GO

--Admire our partitioned table after the SWITCHING is complete.
--We have kept one empty partition sandwiching each end 
--(this is minimum-- you probably want more in production!)
SELECT *
FROM ph.ObjectDetail
WHERE object_name IN ('OrdersDaily')
ORDER BY object_name DESC, partition_number;
GO


-- Deleting Files and FileGroups that are no longer needed

SELECT * FROM sys.filegroups;

ALTER DATABASE PartitionThis REMOVE FILE DailyF2;
ALTER DATABASE PartitionThis REMOVE FILEGROUP DailyFG2;

TRUNCATE TABLE OrdersDaily WITH (PARTITIONS (3,4));
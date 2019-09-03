-- In order to prevent the whole query to run by accident
-- Please Execution SQL by Selection (selecting a chuck of SQL at a time)

DECLARE @msg NVARCHAR(MAX);
SET @msg = N'Did you mean to run this whole script?' + CHAR(10)
    + N'MAKE SURE YOU ARE RUNNING AGAINST A TEST ENVIRONMENT ONLY!'

RAISERROR(@msg,20,1) WITH LOG;
GO

-- Change the database
USE [PartitionThis]
GO


--Create a tally table with ~4 million rows, we'll use this to populate test tables
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

SELECT TOP (1000) * FROM ph.tally;

-- Create Partition Function 
CREATE PARTITION FUNCTION TEST_PartitionFunc (DATETIME2(0))
    AS RANGE RIGHT FOR VALUES
    (
	DATEADD(dd,-4,CAST(SYSDATETIME() AS DATE)), 
	DATEADD(dd,-3,CAST(SYSDATETIME() AS DATE)), 
	DATEADD(dd,-2,CAST(SYSDATETIME() AS DATE)), 
	DATEADD(dd,-1,CAST(SYSDATETIME() AS DATE)), 
	CAST(SYSDATETIME() AS DATE) 
	); -- 6 values 
GO

-- SELECT * FROM sys.partition_functions;

-- Adding FileGroups 
ALTER DATABASE PartitionThis ADD FILEGROUP TEXT_FG1
GO
ALTER DATABASE PartitionThis ADD FILEGROUP TEXT_FG2
GO
ALTER DATABASE PartitionThis ADD FILEGROUP TEXT_FG3
GO
ALTER DATABASE PartitionThis ADD FILEGROUP TEXT_FG4
GO 
ALTER DATABASE PartitionThis ADD FILEGROUP TEXT_FG5
GO
ALTER DATABASE PartitionThis ADD FILEGROUP TEXT_FG6
GO

--Add files to the filegroups
--This is being done dynamically so it will work on different instances, 
--but it makes some big assumptions!
DECLARE @path NVARCHAR(256), @i TINYINT=1, @sql NVARCHAR(4000);
SELECT TOP 1 @path=LEFT(physical_name,LEN(physical_name)-4) 
FROM sys.database_files WHERE name='PartitionThis';

WHILE @i <= 6
BEGIN
	SET @sql=N'ALTER DATABASE PartitionThis ADD FILE (name=TEXT_F' + CAST(@i AS NCHAR(1))+', 
		 filename=''' +  @path + N'TEST_F'+ CAST(@i AS NCHAR(1))+'.ndf' + ''',
		 size=128MB, filegrowth=256MB) TO FILEGROUP TEXT_FG'+CAST(@i AS NCHAR(1))
	--show the command we're running
	RAISERROR (@sql,0,0)
	
	--run it
	EXEC sp_executesql @sql;
	SET @i+=1;
END
GO

-- Adding Partition Scheme
CREATE PARTITION SCHEME TEST_PartitionScheme 
	AS PARTITION TEST_PartitionFunc
	TO (TEXT_FG1, TEXT_FG2, TEXT_FG3, TEXT_FG4, TEXT_FG5, TEXT_FG6);

-- Create TEST tables

DROP TABLE dbo.TEST_with_Partition;
CREATE TABLE dbo.TEST_with_Partition
(
	date_time DATETIME2(0) NOT NULL,
	id INT NOT NULL,
	group_name VARCHAR(10) NOT NULL
) ON TEST_PartitionScheme(date_time)
GO

DROP TABLE dbo.TEST_without_Partition;
CREATE TABLE dbo.TEST_without_Partition
(
	date_time DATETIME2(0) NOT NULL,
	id INT NOT NULL,
	group_name VARCHAR(10) NOT NULL
)
GO

--SELECT TOP (100) number = ROW_NUMBER() OVER (), id FROM dbo.TEST_without_Partition;


-- Populate the tables with data

--INSERT INTO TEST_with_Partition (date_time, id, group_name)
--VALUES (CAST(SYSDATETIME() AS DATETIME2(0)), CAST(RAND(10) AS INT) , 'PGroup1')
--GO 1000000
-- (The above technique is super slow. Use the alternate way below to insert faster)

-- ########################
-- # Table with Partition #
-- ########################

-- Today
DECLARE @today VARCHAR(10) = '20190716';
DECLARE @number_of_rows INT = 1000000;
INSERT INTO TEST_with_Partition (date_time, id, group_name)
SELECT DATEADD(ss, t.N % 86400, CAST(CAST(@today AS DATE) AS DATETIME2(0))) AS date_time,
t.N AS id,
N'PGroup1' AS group_name
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Sanity check:

--SELECT TOP (100) * FROM dbo.TEST_with_Partition;
--SELECT COUNT(*) FROM dbo.TEST_with_Partition
--WHERE date_time >= '20190716';

-- Today - 1
DECLARE @today VARCHAR(10) = '20190716';
DECLARE @number_of_rows INT = 1000000;
INSERT INTO TEST_with_Partition (date_time, id, group_name)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,-1,CAST(CAST(@today AS DATE) AS DATETIME2(0)))) AS date_time,
(t.N + (@number_of_rows * 1)) AS id,
N'PGroup2' AS group_name
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Sanity check:

--SELECT TOP (100) * FROM dbo.TEST_with_Partition;
--SELECT COUNT(*) FROM dbo.TEST_with_Partition
--WHERE date_time >= '20190715' AND date_time < '20190716';

-- Today - 2 
DECLARE @today VARCHAR(10) = '20190716';
DECLARE @number_of_rows INT = 1000000;
DECLARE @day_before INT = 2;
INSERT INTO TEST_with_Partition (date_time, id, group_name)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,-@day_before,CAST(CAST(@today AS DATE) AS DATETIME2(0)))) AS date_time,
(t.N + (@number_of_rows * @day_before)) AS id,
N'PGroup3' AS group_name
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO


-- Sanity check:

--SELECT TOP (100) * FROM dbo.TEST_with_Partition;
--SELECT COUNT(*) FROM dbo.TEST_with_Partition
--WHERE date_time >= '20190714' AND date_time < '20190715';

-- Today - 3 
DECLARE @today VARCHAR(10) = '20190716';
DECLARE @number_of_rows INT = 1000000;
DECLARE @day_before INT = 3;
INSERT INTO TEST_with_Partition (date_time, id, group_name)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,-@day_before,CAST(CAST(@today AS DATE) AS DATETIME2(0)))) AS date_time,
(t.N + (@number_of_rows * @day_before)) AS id,
N'PGroup4' AS group_name
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Sanity check:

--SELECT TOP (100) * FROM dbo.TEST_with_Partition;
--SELECT COUNT(*) FROM dbo.TEST_with_Partition
--WHERE date_time >= '20190713' AND date_time < '20190714';

-- Today - 4 
DECLARE @today VARCHAR(10) = '20190716';
DECLARE @number_of_rows INT = 1000000;
DECLARE @day_before INT = 4;
INSERT INTO TEST_with_Partition (date_time, id, group_name)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,-@day_before,CAST(CAST(@today AS DATE) AS DATETIME2(0)))) AS date_time,
(t.N + (@number_of_rows * @day_before)) AS id,
N'PGroup5' AS group_name
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO


-- Sanity check:

--SELECT TOP (100) * FROM dbo.TEST_with_Partition;
--SELECT COUNT(*) FROM dbo.TEST_with_Partition
--WHERE date_time >= '20190712' AND date_time < '20190713';

--SELECT COUNT(*) FROM dbo.TEST_with_Partition;


-- ###########################
-- # Table without Partition #
-- ###########################

-- Today
DECLARE @today VARCHAR(10) = '20190716';
DECLARE @number_of_rows INT = 1000000;
INSERT INTO TEST_without_Partition (date_time, id, group_name)
SELECT DATEADD(ss, t.N % 86400, CAST(CAST(@today AS DATE) AS DATETIME2(0))) AS date_time,
t.N AS id,
N'PGroup1' AS group_name
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Today - 1
DECLARE @today VARCHAR(10) = '20190716';
DECLARE @number_of_rows INT = 1000000;
INSERT INTO TEST_without_Partition (date_time, id, group_name)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,-1,CAST(CAST(@today AS DATE) AS DATETIME2(0)))) AS date_time,
(t.N + (@number_of_rows * 1)) AS id,
N'PGroup2' AS group_name
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Today - 2 
DECLARE @today VARCHAR(10) = '20190716';
DECLARE @number_of_rows INT = 1000000;
DECLARE @day_before INT = 2;
INSERT INTO TEST_without_Partition (date_time, id, group_name)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,-@day_before,CAST(CAST(@today AS DATE) AS DATETIME2(0)))) AS date_time,
(t.N + (@number_of_rows * @day_before)) AS id,
N'PGroup3' AS group_name
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Today - 3 
DECLARE @today VARCHAR(10) = '20190716';
DECLARE @number_of_rows INT = 1000000;
DECLARE @day_before INT = 3;
INSERT INTO TEST_without_Partition (date_time, id, group_name)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,-@day_before,CAST(CAST(@today AS DATE) AS DATETIME2(0)))) AS date_time,
(t.N + (@number_of_rows * @day_before)) AS id,
N'PGroup4' AS group_name
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Today - 4 
DECLARE @today VARCHAR(10) = '20190716';
DECLARE @number_of_rows INT = 1000000;
DECLARE @day_before INT = 4;
INSERT INTO TEST_without_Partition (date_time, id, group_name)
SELECT DATEADD(ss, t.N % 86400, DATEADD(dd,-@day_before,CAST(CAST(@today AS DATE) AS DATETIME2(0)))) AS date_time,
(t.N + (@number_of_rows * @day_before)) AS id,
N'PGroup5' AS group_name
FROM ph.tally AS t
WHERE N <= @number_of_rows;
GO

-- Sanity check:

--SELECT TOP (100) * FROM dbo.TEST_without_Partition;
--SELECT COUNT(*) FROM dbo.TEST_without_Partition
--SELECT TOP(100) * FROM dbo.TEST_without_Partition
--WHERE date_time >= '20190712' AND date_time < '20190713';

--SELECT COUNT(*) FROM dbo.TEST_without_Partition;
--TRUNCATE TABLE dbo.TEST_with_Partition;
--TRUNCATE TABLE dbo.TEST_without_Partition;


-- Select rows

-- Check the number of rows
DECLARE @start_date VARCHAR(10) = '20190716'; 
DECLARE @end_date VARCHAR(10) = '20190717';
SELECT COUNT(*) FROM dbo.TEST_with_Partition
--SELECT TOP (1000) * FROM dbo.TEST_with_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;

SELECT TOP (100) * FROM dbo.TEST_with_Partition;
SELECT COUNT(*) FROM dbo.TEST_without_Partition;


--SELECT * FROM dbo.TEST_without_Partition;


-- Performance Test
-- # 1 Simple SELECT query

-- Partitioned Table
DECLARE @start_date DATETIME2(0) = '2019-07-14 16:55:29'; 
DECLARE @end_date DATETIME2(0) = '2019-07-16 18:21:00';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_with_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- (2056441 rows affected)
-- Table 'TEST_with_Partition'. Scan count 3, logical reads 11154, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
-- Result: CPU time = 687 ms,  elapsed time = 13790 ms.

-- Non-Partitioned Table 
DECLARE @start_date DATETIME2(0) = '2019-07-14 16:55:29'; 
DECLARE @end_date DATETIME2(0) = '2019-07-16 18:21:00';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_without_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- (2056441 rows affected)
-- Table 'TEST_without_Partition'. Scan count 3, logical reads 18588, physical reads 0, read-ahead reads 0, lob logical reads 0, lob physical reads 0, lob read-ahead reads 0.
-- Result: CPU time = 1578 ms,  elapsed time = 12635 ms.

-- ## Result Summary: In Partitioned Table, we require less number of logical reads. 
-- Queries are also 2.5 times faster on Partition table (considering CPU time). 
-- However, the total elapsed times are similar (Non-Partitioned Table is usually is a little faster because it uses Parallelism)
-- As a result, the faster CPU processing in Partitioned Table is overshadowed by the parallel processing in Non-Partitioned Table

-- Other results:
-- For SELECT query execution across the same partition
-- Elapsed Time: micro seconds 109377 + 93711 + 109377  (w Part)
-- Elapsed Time: micro seconds 390614 + 218754 + 218751 (w/o part)

-- ## Result Summary: In Partition Table, the queries are twice faster due to seeking in the same partition
-- However, these results may vary across partition

-- 12331347
-- 12406277


-- Checking the values in the Wait Stats
-- SELECT TOP (100) * FROM sys.dm_os_wait_stats;

-- # 2 Aggregation SELECT query

-- Partitioned Table
DECLARE @start_date DATETIME2(0) = '2019-07-14 16:55:29'; 
DECLARE @end_date DATETIME2(0) = '2019-07-16 18:21:00';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

DECLARE @TSQL VARCHAR(500);
DECLARE @table_name VARCHAR(50); 

SET @table_name = 'dbo.TEST_with_Partition';

SET @TSQL =
N'SELECT MAX(id), group_name FROM '+@table_name+'
WHERE date_time >= '''+CAST(@start_date AS NVARCHAR)+'''
AND date_time < '''+CAST(@end_date AS NVARCHAR)+'''
GROUP BY group_name;';
--RAISERROR(@TSQL,0,0);

EXEC(@TSQL);

SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;

PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- Result: CPU time = 1563 ms,  elapsed time = 842 ms.

-- Non-Partitioned Table 
DECLARE @start_date DATETIME2(0) = '2019-07-14 16:55:29'; 
DECLARE @end_date DATETIME2(0) = '2019-07-16 18:21:00';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

DECLARE @TSQL VARCHAR(500);
DECLARE @table_name VARCHAR(50); 

SET @table_name = 'dbo.TEST_without_Partition';

SET @TSQL =
N'SELECT MAX(id), group_name FROM '+@table_name+'
WHERE date_time >= '''+CAST(@start_date AS NVARCHAR)+'''
AND date_time < '''+CAST(@end_date AS NVARCHAR)+'''
GROUP BY group_name;';
--RAISERROR(@TSQL,0,0);

EXEC(@TSQL);

SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;

PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));


-- Result: CPU time = 1280 ms,  elapsed time = 634 ms


-- Results Summary: 
-- SQL Server uses Parallelism for both partitioned and non-partitioned tables.
-- Again, queries in the non-partitioned table perform faster than that of partitioned table


-- ****************************************************** 

-- Performance Test 
-- Multiple queries on the same table (across different partition ranges)

-- Partitioned Table

-- Query #1

DECLARE @start_date DATETIME2(0) = '2019-07-12'; 
DECLARE @end_date DATETIME2(0) = '2019-07-13';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_with_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- Query #2

DECLARE @start_date DATETIME2(0) = '2019-07-13'; 
DECLARE @end_date DATETIME2(0) = '2019-07-14';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_with_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- Query #3

DECLARE @start_date DATETIME2(0) = '2019-07-14'; 
DECLARE @end_date DATETIME2(0) = '2019-07-15';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_with_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- Query #4

DECLARE @start_date DATETIME2(0) = '2019-07-15'; 
DECLARE @end_date DATETIME2(0) = '2019-07-16';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_with_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- Query #5

DECLARE @start_date DATETIME2(0) = '2019-07-16'; 
DECLARE @end_date DATETIME2(0) = '2019-07-17';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_with_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));


-- Elapsed time for running all the queries in separate tabs 
-- start_time and end_time are collected from the first and the last query executions

DECLARE @start_time DATETIME2(7) = '2019-07-17 18:24:01.7021673';
DECLARE @end_time DATETIME2(7) = '2019-07-17 18:24:11.9229739';
SELECT DATEDIFF(ms, @start_time, @end_time);

-- Result: Elapsed time: 10220 milliseconds 

-- Non-Partitioned Table

-- Query #1

DECLARE @start_date DATETIME2(0) = '2019-07-12'; 
DECLARE @end_date DATETIME2(0) = '2019-07-13';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_without_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- Query #2

DECLARE @start_date DATETIME2(0) = '2019-07-13'; 
DECLARE @end_date DATETIME2(0) = '2019-07-14';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_without_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- Query #3

DECLARE @start_date DATETIME2(0) = '2019-07-14'; 
DECLARE @end_date DATETIME2(0) = '2019-07-15';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_without_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- Query #4

DECLARE @start_date DATETIME2(0) = '2019-07-15'; 
DECLARE @end_date DATETIME2(0) = '2019-07-16';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_without_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- Query #5

DECLARE @start_date DATETIME2(0) = '2019-07-16'; 
DECLARE @end_date DATETIME2(0) = '2019-07-17';
DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
SELECT * FROM dbo.TEST_without_Partition
WHERE date_time >= @start_date
AND date_time < @end_date;
SET STATISTICS TIME OFF;
SET STATISTICS IO OFF;
PRINT(DATEDIFF(mcs, @time_start, SYSUTCDATETIME()));

-- Elapsed time for running all the queries in separate tabs 
-- start_time and end_time are collected from the first and the last query executions

DECLARE @start_time DATETIME2(7) = '2019-07-17 18:32:46.7383764';
DECLARE @end_time DATETIME2(7) = '2019-07-17 18:32:57.7808498';
SELECT DATEDIFF(ms, @start_time, @end_time);

-- Result: Elapsed time: 11042 milliseconds 

-- Result Summary: 
-- Both partition and non-partition tables have similar query speed
-- However, partition table is a little faster

-- ****************************************************** 

SELECT top (10000) * FROM dbo.TEST_with_Partition
WHERE date_time >= '20190715';

-- Delete/Truncate rows based on values

DECLARE @time_start DATETIME2= SYSUTCDATETIME();
TRUNCATE TABLE dbo.TEST_with_Partition
WITH (PARTITIONS (3));
PRINT(DATEDIFF(ms, @time_start, SYSUTCDATETIME()));

DECLARE @time_start DATETIME2= SYSUTCDATETIME();
DELETE FROM dbo.TEST_without_Partition
WHERE group_name = 'PGroup2'
PRINT(DATEDIFF(ms, @time_start, SYSUTCDATETIME()));



-- Truncate tables

--TRUNCATE TABLE dbo.TEST_with_Partition;
--TRUNCATE TABLE dbo.TEST_without_Partition;

-- drop tables

--DROP TABLE dbo.TEST_with_Partition;
--DROP TABLE dbo.TEST_without_Partition;

SELECT * FROM sys.partition_schemes;
-- [sys].[partition_schemes].data_space_id: 65602
-- [sys].[partition_schemes].function_id: 65537

SELECT * FROM sys.partition_range_values
WHERE function_id = 65537;
-- [sys].[partition_schemes].boundary_id tells us the range values of the Partitions

SELECT * FROM sys.partitions;

SELECT * FROM sys.partitions
WHERE OBJECT_NAME(OBJECT_ID) = 'TEST_with_Partition';
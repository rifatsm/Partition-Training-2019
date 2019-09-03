--******************
--Copyright 2013, Brent Ozar PLF, LLC DBA Brent Ozar Unlimited.
--******************

--******************
--Don't just run the whole thing.
--Run this step by step to learn!
--******************
DECLARE @msg NVARCHAR(MAX);
SET @msg = N'Did you mean to run this whole script?' + CHAR(10)
    + N'MAKE SURE YOU ARE RUNNING AGAINST A TEST ENVIRONMENT ONLY!'

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

--Create a view to see partition information by filegroup
CREATE VIEW ph.FileGroupDetail
AS
SELECT  pf.name AS pf_name ,
        ps.name AS partition_scheme_name ,
        p.partition_number ,
        ds.name AS partition_filegroup ,
        pf.type_desc AS pf_type_desc ,
        pf.fanout AS pf_fanout ,
        pf.boundary_value_on_right ,
        OBJECT_NAME(si.object_id) AS object_name ,
        rv.value AS range_value ,
        SUM(CASE WHEN si.index_id IN ( 1, 0 ) THEN p.rows
                    ELSE 0
            END) AS num_rows ,
        SUM(dbps.reserved_page_count) * 8 / 1024. AS reserved_mb_all_indexes ,
        SUM(CASE ISNULL(si.index_id, 0)
                WHEN 0 THEN 0
                ELSE 1
            END) AS num_indexes
FROM    sys.destination_data_spaces AS dds
        JOIN sys.data_spaces AS ds ON dds.data_space_id = ds.data_space_id
        JOIN sys.partition_schemes AS ps ON dds.partition_scheme_id = ps.data_space_id
        JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
        LEFT JOIN sys.partition_range_values AS rv ON pf.function_id = rv.function_id
                                                        AND dds.destination_id = CASE pf.boundary_value_on_right
                                                                                    WHEN 0 THEN rv.boundary_id
                                                                                    ELSE rv.boundary_id + 1
                                                                                END
        LEFT JOIN sys.indexes AS si ON dds.partition_scheme_id = si.data_space_id
        LEFT JOIN sys.partitions AS p ON si.object_id = p.object_id
                                            AND si.index_id = p.index_id
                                            AND dds.destination_id = p.partition_number
        LEFT JOIN sys.dm_db_partition_stats AS dbps ON p.object_id = dbps.object_id
                                                        AND p.partition_id = dbps.partition_id
GROUP BY ds.name ,
        p.partition_number ,
        pf.name ,
        pf.type_desc ,
        pf.fanout ,
        pf.boundary_value_on_right ,
        ps.name ,
        si.object_id ,
        rv.value;
GO

--Create a view to see partition information by object
CREATE VIEW ph.ObjectDetail	
AS
SELECT  SCHEMA_NAME(so.schema_id) AS schema_name ,
        OBJECT_NAME(p.object_id) AS object_name ,
        p.partition_number ,
        p.data_compression_desc ,
        dbps.row_count ,
        dbps.reserved_page_count * 8 / 1024. AS reserved_mb ,
        si.index_id ,
        CASE WHEN si.index_id = 0 THEN '(heap!)'
                ELSE si.name
        END AS index_name ,
        si.is_unique ,
        si.data_space_id ,
        mappedto.name AS mapped_to_name ,
        mappedto.type_desc AS mapped_to_type_desc ,
        partitionds.name AS partition_filegroup ,
        pf.name AS pf_name ,
        pf.type_desc AS pf_type_desc ,
        pf.fanout AS pf_fanout ,
        pf.boundary_value_on_right ,
        ps.name AS partition_scheme_name ,
        rv.value AS range_value
FROM    sys.partitions p
JOIN    sys.objects so
        ON p.object_id = so.object_id
            AND so.is_ms_shipped = 0
LEFT JOIN sys.dm_db_partition_stats AS dbps
        ON p.object_id = dbps.object_id
            AND p.partition_id = dbps.partition_id
JOIN    sys.indexes si
        ON p.object_id = si.object_id
            AND p.index_id = si.index_id
LEFT JOIN sys.data_spaces mappedto
        ON si.data_space_id = mappedto.data_space_id
LEFT JOIN sys.destination_data_spaces dds
        ON si.data_space_id = dds.partition_scheme_id
            AND p.partition_number = dds.destination_id
LEFT JOIN sys.data_spaces partitionds
        ON dds.data_space_id = partitionds.data_space_id
LEFT JOIN sys.partition_schemes AS ps
        ON dds.partition_scheme_id = ps.data_space_id
LEFT JOIN sys.partition_functions AS pf
        ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values AS rv
        ON pf.function_id = rv.function_id
            AND dds.destination_id = CASE pf.boundary_value_on_right
                                        WHEN 0 THEN rv.boundary_id
                                        ELSE rv.boundary_id + 1
                                    END
GO


--Create a tally table with ~4 million rows, we'll use this to populate test tables
--This general method attributed to Itzik Ben-Gan
;WITH    Pass0 AS ( SELECT   1 AS C UNION ALL SELECT   1), 
	Pass1 AS ( SELECT   1 AS C FROM     Pass0 AS A , Pass0 AS B),
	Pass2 AS ( SELECT   1 AS C FROM     Pass1 AS A , Pass1 AS B),
	Pass3 AS ( SELECT   1 AS C FROM     Pass2 AS A , Pass2 AS B),
	Pass4 AS ( SELECT   1 AS C FROM     Pass3 AS A , Pass3 AS B),
	Pass5 AS ( SELECT   1 AS C FROM     Pass4 AS A , Pass4 AS B),
	tally AS ( SELECT   row_number() OVER ( Order BY C ) AS N FROM Pass5 )
SELECT  N
INTO    ph.tally_ten_thousands
FROM    tally
WHERE   N <= 10000;
GO

-- SELECT COUNT(*) FROM ph.tally_ten_thousands;
-- SELECT * FROM ph.tally_ten_thousands;
-- DROP TABLE ph.tally_ten_thousands;



--******************
--3. CREATE OUR HERO, THE PARTITION FUNCTION
--Daily Example: RIGHT bound partition function 
--Cool point: It can use variables and functions
--******************

-- Drop existing Partiton Scheme and Partition Function 

SELECT * FROM sys.partition_functions;

DROP PARTITION FUNCTION DailyPF;

--Create the partition function: dailyPF
DECLARE @StartDay DATE=DATEADD(dd,-3,CAST(SYSDATETIME() AS DATE));
CREATE PARTITION FUNCTION DailyPF (DATETIME2(0))
    AS RANGE RIGHT FOR VALUES
    (@StartDay, 
	DATEADD(dd,1,@StartDay), 
	DATEADD(dd,2,@StartDay),  
	DATEADD(dd,3,@StartDay), 
	DATEADD(dd,4,@StartDay) ); -- 5 values 
GO

--When typing dates to create a partition, use ODBC standard date format 


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

--SELECT TOP (1000) * FROM ph.FileGroupDetail;
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

DROP PARTITION SCHEME DailyPS;

--Create the partition scheme: dailyPS 
CREATE PARTITION SCHEME DailyPS 
	AS PARTITION DailyPF
	TO (DailyFG1, DailyFG2, DailyFG3, DailyFG4, DailyFG5, DailyFG6);

--Look at how this is mapped out now
SELECT *
FROM ph.FileGroupDetail;
GO



--******************
--6. CREATE OBJECTS ON THE PARTITION SCHEME
--******************

--Create a partitioned heap... yep, you can do that!
--When would a partitioned heap be useful?
--What could go wrong with a partitioned heap?
if OBJECT_ID('OrdersDaily','U') is null
CREATE TABLE OrdersDaily (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on DailyPS(OrderDate)
GO

if OBJECT_ID('OrdersDaily_Without_Partition','U') is null
CREATE TABLE OrdersDaily_Without_Partition  (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY NOT NULL,
	OrderName nvarchar(256) NOT NULL
) 
GO


--******************
--6. INSERT ROWS
--******************
--Where would records go for different days?
--You can use the $PARTITION function
SELECT $PARTITION.DailyPF( DATEADD(dd,-100,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumber100DaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,-7,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberSevenDaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,-3,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberThreeDaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,-2,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberTwoDaysAgo,
	$PARTITION.DailyPF( DATEADD(dd,1,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberTomorrow,
	$PARTITION.DailyPF( DATEADD(dd,7,CAST(SYSDATETIME() AS DATETIME2(0)))) AS PartitionNumberNextWeek
GO


--Let's insert some rows!
--We are leaving Partition 1 and Partition 6 empty on purpose-- 
--it's a best practice to have empty partitions at each end.

-- Three days ago = 1000 rows
DECLARE @number_of_rows INT = 10000;
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, DATEADD(dd,-3,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Robot' WHEN t.N % 4 = 0 THEN 'Badger'  ELSE 'Pen' END AS OrderName
FROM ph.tally_ten_thousands AS t
WHERE N < = @number_of_rows;
	
--Two days ago = 2000 rows
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, DATEADD(dd,-2,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Flying Monkey' WHEN t.N % 4 = 0 THEN 'Junebug'  ELSE 'Pen' END AS OrderName
FROM ph.tally_ten_thousands AS t
WHERE N < = @number_of_rows;

--Yesterday= 3000 rows
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, DATEADD(dd,-1,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 2 = 0 THEN 'Turtle' WHEN t.N % 5 = 0 THEN 'Eraser'  ELSE 'Pen' END AS OrderName
FROM ph.tally_ten_thousands AS t
WHERE N < = @number_of_rows;

--Today=  4000 rows
INSERT OrdersDaily(OrderDate, OrderName) 
SELECT DATEADD(ss, t.N, CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Lasso' WHEN t.N % 2 = 0 THEN 'Cattle Prod'  ELSE 'Pen' END AS OrderName
FROM ph.tally_ten_thousands AS t
WHERE N < = @number_of_rows;
GO

-- SELECT COUNT(*) FROM dbo.OrdersDaily;
-- TRUNCATE TABLE dbo.OrdersDaily;

--Now look at our heap
--This is where those helper objects come in handy.
--DailyFG1 and DailyFG6 are both empty as planned.
SELECT *
FROM ph.ObjectDetail
WHERE object_name='OrdersDaily'
order by partition_number;
GO


--******************
--7. LET'S ADD SOME INDEXES ....
--******************
--Add a Clustered Index-- Not a heap anymore
ALTER TABLE OrdersDaily
ADD CONSTRAINT PKOrdersDaily
	PRIMARY KEY CLUSTERED(OrderDate,OrderId)
GO


--An aligned NCI
--We don't have to specify the partition function.
CREATE NONCLUSTERED INDEX NCOrderIdOrdersDaily
	ON OrdersDaily(OrderId)
GO


--An NCI that is NOT aligned
CREATE NONCLUSTERED INDEX NCOrderNameOrdersDailyNonAligned 
	ON OrdersDaily(OrderName) ON [PRIMARY]
GO

--Look at the CI and NCs
SELECT partition_number, row_count, range_value, reserved_mb, 
	index_id, index_name,mapped_to_name,mapped_to_type_desc, partition_filegroup, pf_name
FROM ph.ObjectDetail
WHERE object_name='OrdersDaily'
order by index_name, partition_number;
--compare to:
EXEC sp_helpindex OrdersDaily

----------------- @Ri - Rough BEGIN -----------------

--TRUNCATE TABLE dbo.OrdersDaily;

SELECT COUNT(*) FROM dbo.OrdersDaily;
--SELECT COUNT(*) FROM OrdersDaily_Without_Partition;
SELECT TOP (100) * FROM dbo.OrdersDaily;

DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SELECT * FROM dbo.OrdersDaily
WHERE OrderId = 4
OR OrderId = 6100
OR OrderId = 3705
OR OrderId = 1003
OR OrderId = 2000002
OR OrderId = 3000003
OR OrderId = 3000002
OR OrderId = 1000004
OR OrderId = 2000005;
PRINT(DATEDIFF(ns, @time_start, SYSUTCDATETIME()));

--SELECT TOP (100) * FROM dbo.OrdersDaily
--WHERE OrderName = 'Eraser'

DECLARE @time_start_2 DATETIME2= SYSUTCDATETIME();
SELECT * FROM dbo.OrdersDaily_Without_Partition
WHERE OrderId = 4
OR OrderId = 6100
OR OrderId = 3705
OR OrderId = 1003
OR OrderId = 2000002
OR OrderId = 3000003
OR OrderId = 3000002
OR OrderId = 1000004
OR OrderId = 2000005;
PRINT(DATEDIFF(ns, @time_start_2, SYSUTCDATETIME()));


DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SELECT * FROM dbo.OrdersDaily
WHERE OrderDate <= DATEADD(dd,-2,CAST(CAST(SYSDATETIME() AS DATE) AS DATETIME2(0)))
PRINT(DATEDIFF(ns, @time_start, SYSUTCDATETIME()));

SELECT COUNT(*) FROM dbo.OrdersDaily;

DECLARE @time_start DATETIME2= SYSUTCDATETIME();
SELECT * FROM dbo.OrdersDaily_Without_Partition
WHERE OrderName = 'Badger'
PRINT(DATEDIFF(ns, @time_start, SYSUTCDATETIME()));

----------------- @Ri - Rough END -----------------

--******************
--8. SWITCHING IN NEW PARTITIONS...
--Like lightning.
--******************

--I want to load data for tomorrow and then switch it in.
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



--Create a staging table on our new filegroup (dailyFG6)
--Why are we seeding the identity here?
--What would happen if we didn't?
--Ans: https://dba.stackexchange.com/questions/183237/partition-switch-what-is-the-benefit-if-staging-has-to-be-same-as-destination/183243
--this reduces the amount of contention for locks/resource on the primary table
CREATE TABLE OrdersDailyLoad (
	OrderDate DATETIME2(0) NOT NULL,
	OrderId int IDENTITY (10001,1) NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on [DailyFG6]
GO


--Insert some records into our staging table
--Tomorrow=5000 rows
INSERT OrdersDailyLoad(OrderDate, OrderName) 
SELECT DATEADD(SECOND, t.N, 
	DATEADD(dd,1,CAST(SYSDATETIME() AS DATETIME2(0)))) AS OrderDate,
	CASE WHEN t.N % 3 = 0 THEN 'Bow and Arrow' WHEN t.N % 2 = 0 
		THEN 'First Aid Kit'  
		ELSE 'Pen' 
	END AS OrderName
FROM ph.tally_ten_thousands AS t
WHERE N < = 5000
GO

--Create indexes on our staging table
ALTER TABLE OrdersDailyLoad
ADD CONSTRAINT PKOrdersDailyLoad
	PRIMARY KEY CLUSTERED(OrderDate,OrderId)
GO

--Create the aligned NC as well. It can have a different name.
CREATE NONCLUSTERED INDEX NCOrderIdOrdersDailyLoad ON OrdersDailyLoad(OrderId)
GO


--Create two check constraints on the staging table.
--This will ensure data fits in with the allowed range 
--for the partition we want to put it in
--Constraints WITH CHECK are required for switching in

--Create one constraint for the "low end"
DECLARE @tsql NVARCHAR(2000)=
'ALTER TABLE OrdersDailyLoad
WITH CHECK
ADD CONSTRAINT CKOrdersDailyLoad_LowEnd
CHECK (OrderDate >= ''' + 
	convert(CHAR(10),DATEADD(dd,1,CAST(SYSDATETIME() AS DATE))) + ''')'
--Display what we're running
RAISERROR (@tsql,0,0)
--Run it
EXEC sp_executesql @tsql;
GO

--Create one constraint for the "high end"
DECLARE @tsql NVARCHAR(2000)=
'ALTER TABLE OrdersDailyLoad
WITH CHECK
ADD CONSTRAINT CKOrdersDailyLoad_HighEnd
CHECK (OrderDate < ''' + 
	convert(CHAR(10),DATEADD(dd,2,CAST(SYSDATETIME() AS DATE))) + ''')'
--Display what we're running
RAISERROR (@tsql,0,0)
--Run it
EXEC sp_executesql @tsql;




--Homework: insert some rows into OrdersDaily itself for the day 
--you're loading into the staging table-- and see what happens to them.


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
ALTER PARTITION FUNCTION DailyPF() 
SPLIT RANGE (DATEADD(dd,3,CAST(SYSDATETIME() AS DATE)))
GO

-- SELECT * FROM ph.FileGroupDetail;
-- SELECT (DATEADD(dd,2,CAST(SYSDATETIME() AS DATE)));


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
SWITCH TO OrdersDaily PARTITION 6


-- Error msg: 
--Msg 7733, Level 16, State 4, Line 617
--'ALTER TABLE SWITCH' statement failed. The table 'PartitionThis.dbo.OrdersDaily' is partitioned while index 'NCOrderNameOrdersDailyNonAligned' is not partitioned.


--Uh oh...
--We must disable (or drop) this non-aligned index to make switching work
ALTER INDEX NCOrderNameOrdersDailyNonAligned ON OrdersDaily DISABLE;
GO

--Switch in!
ALTER TABLE OrdersDailyLoad
SWITCH TO OrdersDaily PARTITION 6;
GO


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
	OrderId int IDENTITY NOT NULL,
	OrderName nvarchar(256) NOT NULL
) on [DailyFG2];
GO

--Create the primary key our switch out table
ALTER TABLE OrdersDailyOut
ADD CONSTRAINT PKOrdersDailyOut
	PRIMARY KEY CLUSTERED(OrderDate,OrderId);
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



--Note: when switching out to an empty table, we needed to add the clustered index. 
--However, we did NOT need to add NCIs or a check constraint. (Whew!)


--We want to keep an empty partition on DailyFG1
--But we want to remove the empty partition on DailyFG2 (currently Partition 2)
--Programmatically find the boundary point to merge
--This is done so we don't have to hard code dates in the script



DECLARE @MergeBoundaryPoint DATETIME2(0), @msg NVARCHAR(2000);
SELECT @MergeBoundaryPoint = CAST(MIN(range_val.value) AS DATETIME2(0))
FROM sys.partition_functions  part_func
JOIN sys.partition_range_values range_val ON part_func.function_id=range_val.function_id
where part_func.name='DailyPF'

--PRINT(@MergeBoundaryPoint);

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

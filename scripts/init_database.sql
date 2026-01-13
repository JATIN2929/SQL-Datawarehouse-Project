/* 
======================================================================
Create Database and Schemas
======================================================================
Script Purpose: 
    This script creates a new database named 'DataWarehosue' after checking if it already exisits.
    if the database exists, it is dropped and recreated. Aditionally,the script sets up three schema within the database: 'bronze', 'silver' , 'gold'
WARNING: 
    Running this sript will drop the entire 'DataWarehouse' database if it exists
    All data in the database will be deleted permanently. Proceed with caution.
    and ensure you have proper backups before running th scripts.
*/

USE master;
GO

-- dROP AND RECREATE THE DATABASE 'DATAWAREHOUSE' IF IT EXISTS
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    DROP DATABASE DataWarehouse;
END
GO
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- CREATE SCHEMAS FOR DIFFERENT LAYERS OF THE DATA WAREHOUSE
CREATE SCHEMA BRONZE;
GO
CREATE SCHEMA SILVER;
GO
CREATE SCHEMA GOLD;
GO
 

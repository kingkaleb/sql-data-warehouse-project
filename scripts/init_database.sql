use master;
GO

--Drop and recreate the 'DataWarehouse' database
IF EXISTS(SELECT 1 FROM sys.databases where name ='DataWarehouse')
begin
	alter DATABASE DataWarehouse set Single_user with rollback immediate;
	drop database DataWarehouse;
end;
Go
--create the 'DataWarehouse' database
create database Datawarehouse;
GO

use DataWarehouse;
GO

create schema bronze;
GO

create schema silver;
GO

create schema gold;
GO

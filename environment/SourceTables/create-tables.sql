CREATE DATABASE [CdcToRedshift]
GO

USE [CdcToRedshift]
GO

CREATE TABLE [dbo].[Person](
	[PersonId] [int] NOT NULL,
	[FirstName] [varchar](50) NOT NULL,
	[Surname] [varchar](50) NOT NULL,
	[DateOfBirth] [datetime] NOT NULL,
	PRIMARY KEY CLUSTERED([PersonId] ASC)
)
GO

CREATE TABLE [dbo].[PersonAddress](
	[AddressId] [int] NOT NULL,
	[Addressline1] [nvarchar](1000) NOT NULL,
	[City] [nvarchar](100) NOT NULL,
	[Postalcode] [nvarchar](20) NOT NULL,
	[Country] [nvarchar](100) NOT NULL,
	[PersonId] [int] NOT NULL,
	PRIMARY KEY CLUSTERED([AddressId] ASC)
)
GO

-- enable CDC on this database
sys.sp_cdc_enable_db 

-- enable CDC on the Person table
EXEC sys.sp_cdc_enable_table  
@source_schema = N'dbo',  
@source_name   = N'Person',  
@role_name     = NULL,  
@supports_net_changes = 1 

-- enable CDC on the PersonAddress table
EXEC sys.sp_cdc_enable_table  
@source_schema = N'dbo',  
@source_name   = N'PersonAddress',  
@role_name     = NULL,  
@supports_net_changes = 1 
GO
CREATE TABLE [CdcTools].[FullLoadState](
	[ExecutionId] [varchar](50) NOT NULL,
	[TableName] [varchar](200) NOT NULL,
	[PrimaryKeyValue] [varchar](max) NULL,
	[LastUpdate] [datetime] NOT NULL,
	PRIMARY KEY CLUSTERED 
	(
		[ExecutionId] ASC,
		[TableName] ASC
	)
)

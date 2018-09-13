CREATE TABLE [CdcTools].[ChangeState](
	[ExecutionId] [varchar](50) NOT NULL,
	[TableName] [varchar](200) NOT NULL,
	[Lsn] [binary](10) NOT NULL,
	[SeqVal] [binary](10) NOT NULL,
	[LastUpdate] [datetime] NOT NULL,
	PRIMARY KEY CLUSTERED 
	(
		[ExecutionId] ASC,
		[TableName] ASC
	)
)

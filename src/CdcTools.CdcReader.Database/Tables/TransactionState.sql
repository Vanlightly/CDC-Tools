CREATE TABLE [CdcTools].[TransactionState](
	[ExecutionId] [varchar](50) NOT NULL,
	[Lsn] [binary](10) NOT NULL,
	[LastUpdate] [datetime] NOT NULL,
	PRIMARY KEY CLUSTERED 
	(
		[ExecutionId] ASC
	)
)

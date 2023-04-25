/****** Object:  Table [dbo].[ControlTableIntegrated]    Script Date: 2/24/2023 1:17:20 PM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[ControlTableIntegrated]') AND type in (N'U'))
DROP TABLE [dbo].[ControlTableIntegrated]
GO

/****** Object:  Table [dbo].[ControlTableIntegrated]    Script Date: 2/24/2023 1:17:20 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[ControlTableIntegrated](
	[TableID] [int] NOT NULL,
	[SourceServerName] [varchar](255) NULL,
	[SourceDBName] [varchar](255) NULL,
	[SourceTableName] [varchar](255) NULL,
	[FolderPath] [varchar](800) NULL,
	[SchemaName] [varchar](50) NULL,
	[PartitionColumnName] [varchar](255) NULL,
	[PartitionLowerBound] [varchar](50) NULL,
	[PartitionUpperBound] [varchar](50) NULL,
	[ADLSContainerName] [varchar](255) NULL,
	[ADLSStoragePath] [varchar](255) NULL,
	[ADLSArchivalContainer] [varchar](255) NULL,
	[StagingContainer] [varchar](255) NULL,
	[CopyMode] [varchar](50) NULL,
	[DeltaColumnName] [varchar](255) NULL,
	[IsActive] [varchar](10) NULL,
	[WaterMarkColumnName] [varchar](100) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[IngestionLog]    Script Date: 2/24/2023 1:18:51 PM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[IngestionLog]') AND type in (N'U'))
DROP TABLE [dbo].[IngestionLog]
GO

/****** Object:  Table [dbo].[IngestionLog]    Script Date: 2/24/2023 1:18:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[IngestionLog](
	[Id] [bigint] IDENTITY(1,1) NOT NULL,
	[ServerName] [varchar](100) NULL,
	[DbName] [varchar](100) NULL,
	[TableName] [varchar](100) NULL,
	[RunStatus] [varchar](50) NULL,
	[SourceCount] [bigint] NULL,
	[BronzeCount] [bigint] NULL,
	[UpdateDate] [datetime] NULL,
	[ErrorMessage] [varchar](max) NULL,
 CONSTRAINT [PK_IngestionLog] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[table_store_CDC_version]    Script Date: 2/24/2023 1:19:17 PM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[table_store_CDC_version]') AND type in (N'U'))
DROP TABLE [dbo].[table_store_CDC_version]
GO

/****** Object:  Table [dbo].[table_store_CDC_version]    Script Date: 2/24/2023 1:19:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[table_store_CDC_version](
	[ServerName] [varchar](255) NULL,
	[TableName] [varchar](255) NULL,
	[CDCLastRun] [varchar](255) NULL
) ON [PRIMARY]
GO


/****** Object:  Table [dbo].[table_store_ChangeTracking_version]    Script Date: 2/24/2023 1:20:05 PM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[table_store_ChangeTracking_version]') AND type in (N'U'))
DROP TABLE [dbo].[table_store_ChangeTracking_version]
GO

/****** Object:  Table [dbo].[table_store_ChangeTracking_version]    Script Date: 2/24/2023 1:20:05 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[table_store_ChangeTracking_version](
	[TableName] [varchar](255) NULL,
	[SYS_CHANGE_VERSION] [bigint] NULL
) ON [PRIMARY]
GO


/****** Object:  Table [dbo].[table_store_watermark_value]    Script Date: 2/24/2023 1:20:29 PM ******/
IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[table_store_watermark_value]') AND type in (N'U'))
DROP TABLE [dbo].[table_store_watermark_value]
GO

/****** Object:  Table [dbo].[table_store_watermark_value]    Script Date: 2/24/2023 1:20:29 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[table_store_watermark_value](
	[ServerName] [varchar](255) NULL,
	[TableName] [varchar](255) NULL,
	[WatermarkValue] [datetime] NULL
) ON [PRIMARY]
GO


/****** Object:  StoredProcedure [dbo].[InsertLog]    Script Date: 2/24/2023 1:21:11 PM ******/
DROP PROCEDURE [dbo].[InsertLog]
GO

/****** Object:  StoredProcedure [dbo].[InsertLog]    Script Date: 2/24/2023 1:21:11 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:      <Author, , Name>
-- Create Date: <Create Date, , >
-- Description: <Description, , >
-- =============================================
--drop procedure InsertLog
CREATE PROCEDURE [dbo].[InsertLog]
(
    -- Add the parameters for the stored procedure here
     @ServerName   varchar (100) NULL,
	 @DbName   varchar (100) NULL,
	 @TableName   varchar (100) NULL,
	 @RunStatus   varchar (50) NULL,
	 @SourceCount   bigint  NULL,
	 @BronzeCount   bigint  NULL,
	 @error varchar(max)
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON



INSERT INTO  dbo.IngestionLog 
           ( ServerName 
           , DbName 
           , TableName 
           , RunStatus 
           , SourceCount 
           , BronzeCount 
		   ,ErrorMessage
           , UpdateDate )
     VALUES
           (@ServerName,  
           @DbName,  
           @TableName,  
           @RunStatus,
           @SourceCount,
           @BronzeCount, 
		   @error,
           GETDATE())




END

GO


/****** Object:  StoredProcedure [dbo].[Update_CDCLSN_Version]    Script Date: 2/24/2023 1:21:42 PM ******/
DROP PROCEDURE [dbo].[Update_CDCLSN_Version]
GO

/****** Object:  StoredProcedure [dbo].[Update_CDCLSN_Version]    Script Date: 2/24/2023 1:21:42 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[Update_CDCLSN_Version]  @serverName varchar(255), @TableName varchar(255), @lsn varchar(50)
AS
BEGIN

IF EXISTS(SELECT 1 FROM dbo.table_store_CDC_version WHERE ServerName = @serverName and TableName=@TableName)
     UPDATE dbo.table_store_CDC_version 
     set [CDCLastRun] = @lsn
	 where ServerName = @serverName and TableName=@TableName
ELSE
     INSERT INTO dbo.table_store_CDC_version
          (ServerName, TableName, CDCLastRun)
     VALUES
          (@serverName,@TableName,@lsn)


END

GO


/****** Object:  StoredProcedure [dbo].[Update_ChangeTracking_Version]    Script Date: 2/24/2023 1:22:03 PM ******/
DROP PROCEDURE [dbo].[Update_ChangeTracking_Version]
GO

/****** Object:  StoredProcedure [dbo].[Update_ChangeTracking_Version]    Script Date: 2/24/2023 1:22:03 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[Update_ChangeTracking_Version] @CurrentTrackingVersion BIGINT, @TableName varchar(50)
AS
BEGIN
UPDATE table_store_ChangeTracking_version
SET [SYS_CHANGE_VERSION] = @CurrentTrackingVersion
WHERE [TableName] = @TableName
END
GO


/****** Object:  StoredProcedure [dbo].[Update_WaterMark_Value]    Script Date: 2/24/2023 1:22:19 PM ******/
DROP PROCEDURE [dbo].[Update_WaterMark_Value]
GO

/****** Object:  StoredProcedure [dbo].[Update_WaterMark_Value]    Script Date: 2/24/2023 1:22:19 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[Update_WaterMark_Value]  @serverName varchar(255), @TableName varchar(255), @watermark datetime
AS
BEGIN

IF EXISTS(SELECT 1 FROM dbo.table_store_watermark_value WHERE ServerName = @serverName and TableName=@TableName)
     UPDATE dbo.table_store_watermark_value 
     set [WatermarkValue] = @watermark
	 where ServerName = @serverName and TableName=@TableName
ELSE
     INSERT INTO dbo.table_store_watermark_value
          (ServerName, TableName, WatermarkValue)
     VALUES
          (@serverName,@TableName,@watermark)


END

GO




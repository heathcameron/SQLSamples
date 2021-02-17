USE [QAS_PRD_PAR_CUSTOM]
GO

/****** Object:  Table [dbo].[UT_OIL_PROD]    Script Date: 7/29/2020 7:36:40 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
--DROP TABLE [dbo].[UT_OIL_PROD]
CREATE TABLE [dbo].[UT_OIL_PROD](
	[WELL_NO] [varchar](10) NULL,
	[WELL_NAME] [varchar](60) NULL,
	[MONTH] [datetime] NULL,
	[STATUS] [nvarchar](255) NULL,
	[BEG_INV] [numeric](13, 2) NULL,
	[PROD] [numeric](13, 2) NULL,
	[SALES] [numeric](13, 2) NULL,
	[SKIM] [numeric](13, 2) NULL,
	[END_INV] [numeric](13, 2) NULL,
	[GRAV] [numeric](13, 2) NULL, 
	[PURCH] [varchar](60) 
) ON [PRIMARY]
GO



USE [QAS_PRD_PAR_CUSTOM]
GO

/****** Object:  Table [dbo].[UT_LANDS_REPORT]    Script Date: 7/30/2020 10:26:18 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[UT_LANDS_VERIFY_RPT](
	[PROP_NO] [varchar](10) NULL,
	[LSE_NO] [varchar](6) NULL,
	[UNIT_NO] [varchar](10) NULL,
	[RD_AMT_PROP_GRS_VAL] [numeric](13, 2) NULL,
	[RPT_AMT_PROP_GRS_VAL] [numeric](13, 2) NULL,
	[RD_AMT_PROP_GRS_VOL] [numeric](13, 2) NULL,
	[RPT_AMT_PROP_GRS_VOL] [numeric](13, 2) NULL,
	[RD_PRICE] [numeric](13, 2)  NULL,
	[RPT_PRICE] [numeric](13, 2) NULL,
	[RD_UT_ROY_DUE] [numeric](13, 2)  NULL,
	[RPT_UT_ROY_DUE] [numeric](13, 2)  NULL,
	[UT DIFF] [numeric](13, 2)  NULL

) ON [PRIMARY]
GO

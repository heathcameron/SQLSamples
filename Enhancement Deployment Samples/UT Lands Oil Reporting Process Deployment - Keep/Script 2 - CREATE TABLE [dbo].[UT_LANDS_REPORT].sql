USE [QAS_PRD_PAR_CUSTOM]
GO

/****** Object:  Table [dbo].[UT_LANDS_REPORT]    Script Date: 7/29/2020 7:33:37 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[UT_LANDS_REPORT](
	[Report Type] [varchar](3) NULL,
	[Line Type] [varchar](10) NULL,
	[Product] [varchar](5) NULL,
	[PROD Date] [varchar](7) NULL,
	[RRC Type] [varchar](3) NULL,
	[RRC District] [varchar](2) NULL,
	[RRC / Permit #] [varchar](6) NULL,
	[Unit #] [varchar](10) NULL,
	[Univ Lse #] [varchar](10) NULL,
	[Beg Inv#] [numeric](13, 2) NULL,
	[8/8 Production] [numeric](13, 2) NULL,
	[8/8 Disposition] [numeric](13, 2) NULL,
	[End Inv] [numeric](13, 2) NULL,
	[Your Volume] [numeric](13, 2) NULL,
	[8/8 Notes] [nvarchar](255) NULL,
	[API Gravity] [numeric](13, 2) NULL,
	[Oil Type] [varchar](1) NULL,
	[BTU Factor] [numeric](6, 6) NULL,
	[Mkt Val] [numeric](13, 2) NULL,
	[Roy Due] [numeric](13, 2) NULL,
	[Tract LSE] [nvarchar](20) NULL,
	[Tract RRC Number] [varchar](6) NULL,
	[Trace No#] [varchar](15) NULL,
	[MKT Val1] [numeric](13, 2) NULL,
	[Roy Due1] [numeric](13, 2) NULL,
	[Disp Code] [varchar](2) NULL,
	[Volume] [numeric](13, 2) NULL,
	[MKT Val2] [numeric](13, 2) NULL,
	[ROY Due2] [numeric](13, 2) NULL,
	[Affiliated?] [varchar](1) NULL,
	[Purchaser] [varchar](10) NULL,
	[Payor] [varchar](10) NULL
) ON [PRIMARY]
GO



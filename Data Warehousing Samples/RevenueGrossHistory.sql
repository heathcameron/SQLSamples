USE [ParsleyStage]
GO

/****** Object:  StoredProcedure [dbo].[uspLoadFactRevenueGrossHistory]    Script Date: 1/4/2021 11:10:13 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



-- =======================================================================================
-- Description:	This stored procedure loads the [FactRevenueGrossHistory] table
--	 and logs the row count in the ETLControl database.
--
-- Sample invocation:
-- Exec [dbo].[uspLoadFactRevenueGrossHistory] 0, 0, 0, 0
-- ======================================================================================

CREATE OR ALTER PROCEDURE [dbo].[uspLoadFactRevenueGrossHistory]
(
	@pETLGroupID INT,
	@pETLGroupJobID INT,
	@pETLGroupExecutionID INT,
	@AuditEventID INT OUTPUT
)
AS
BEGIN
	DECLARE @ETLGroupID INT = @pETLGroupID
	DECLARE @ETLGroupJobID INT = @pETLGroupJobID
	DECLARE @ETLGroupExecutionID INT = @pETLGroupExecutionID
	DECLARE @Description varchar(5000)
	DECLARE @TargetTable varchar(50) = '[dbo].[FactRevenueGrossHistory]'
	
	---------------------------------------------------------------------------
	-- Create starting Audit Event.
	---------------------------------------------------------------------------
	SET @Description = 'Started ' + @TargetTable + ' Load'
	EXEC ETLControl.ETL.usp_CreateAuditEvent @ETLGroupID , @ETLGroupJobID, @ETLGroupExecutionID, 154
	, NULL, @Description, @AuditEventID OUTPUT
	
	---------------------------------------------------------------------------
	-- Fully reload the Fact table every time.
	---------------------------------------------------------------------------
	TRUNCATE TABLE ParsleyDW.dbo.FactRevenueGrossHistory

	---------------------------------------------------------------------------
	-- Insert Data into Fact table.
	---------------------------------------------------------------------------
	SET @Description = '[QAS].[RTRN_RD_OWNR_LVL_PSUM_HIST] and other tables to ' + @TargetTable + ' Load'

	-- Insert records from regular tables.
	INSERT INTO ParsleyDW.dbo.FactRevenueGrossHistory
	(	
		AuditEventID,
		BKBchSelSeqNo,
		BKIntrnlTransSub,
		BKRvnuRunID,
		AccountingDateID,
		CompanyID,
		DOIID,
		ForemanID,
		OilTransporterID,
		PowerMethodID,
		ProductID,
		ProductionDateID,
		PropertyID,
		PumperID,
		PurchaserID,
		SWDID,
		SWDMethodID,
		UserID,
		WellAgeID,
		BOE,
		Gross,
		MCFE,
		Net,
		Other,
		Price,
		Tax,
		Volume
	)
	SELECT 
		@AuditEventID,
		NULL,
		rh.TRANS_NO,
		rh.RVNU_RUN_ID,
		COALESCE(dda.DateID, -1) AS AccountingDateID,
		COALESCE(dc.CompanyID, -1) AS CompanyID,
		COALESCE(ddoi.DOIID, -1) AS DOIID,
		COALESCE(foreman.ForemanID, -1) AS ForemanID,
		COALESCE(ot.OilTransporterID, -1) AS OilTransporterID,
		COALESCE(pm.PowerMethodID, -1) AS PowerMethodID,
		COALESCE(dpr.ProductID, -1) AS ProductID,
		COALESCE(ddp.DateID, -1) AS ProductionDateID,
		COALESCE(dp.PropertyID, -1) AS PropertyID,
		COALESCE(pumper.PumperID, -1) AS PumperID,
		COALESCE(dpu.PurchaserID, -1) AS PurchaserID,
		COALESCE(swd.SWDID, -1) AS SWDID,
		COALESCE(sm.SWDMethodID, -1) AS SWDMethodID,
		COALESCE(du.UserID, -1) AS UserID,
		CASE WHEN DATEDIFF(mm, dp.FirstProductionDate, ddp.Date) < 0 AND dp.FirstProductionDate <> '1900-01-01' AND ddp.Date <> '1900-01-01' 
			THEN -2 
			ELSE COALESCE(wa.WellAgeID, -1) 
		END AS WellAgeID,
		CASE rh.UOM_CD
			WHEN 'BBL' THEN rh.ACT_OWNR_GRS_VOL
			WHEN 'MCF' THEN rh.ACT_OWNR_GRS_VOL / 6.0
			ELSE 0
		END AS BOE,
		rh.ACT_OWNR_GRS_AMT AS Gross,
		CASE rh.UOM_CD
			WHEN 'BBL' THEN rh.ACT_OWNR_GRS_VOL * 6.0
			WHEN 'MCF' THEN rh.ACT_OWNR_GRS_VOL
			ELSE 0
		END AS MCFE,
		(rh.ACT_OWNR_GRS_AMT - ISNULL(tax.Tax, 0) - ISNULL(adj.Other, 0)) AS Net,
		adj.Other AS Other,
		CASE WHEN (rh.ACT_OWNR_GRS_VOL IS NULL OR rh.ACT_OWNR_GRS_VOL = 0) THEN NULL ELSE (rh.ACT_OWNR_GRS_AMT / rh.ACT_OWNR_GRS_VOL) END AS Price,
		tax.Tax AS Tax,
		rh.ACT_OWNR_GRS_VOL AS Volume
	FROM QAS.RTRN_RD_OWNR_LVL_PSUM_HIST rh
		INNER JOIN QAS.JTRN_JEPOST_PROC posted ON
			rh.RVNU_RUN_ID = posted.RVNU_RUN_ID
			AND posted.RVNU_STAT_CD = 'PST' -- Only get posted revenue	
		LEFT JOIN
		(
			SELECT RVNU_RUN_ID, TRANS_NO, (SUM(ACT_OWNR_TAX_AMT) + SUM(ACT_OWNR_TAX_REIMB_AMT)) AS Tax		--- 6/28 - Added ACT_OWNR_TAX_REIMB_AMT
			FROM QAS.RTRN_RD_OWNR_LVL_TAX_PSUM_HIST
			GROUP BY RVNU_RUN_ID, TRANS_NO
		) tax ON
			rh.RVNU_RUN_ID = tax.RVNU_RUN_ID
			AND rh.TRANS_NO = tax.TRANS_NO
		LEFT JOIN
		(
			SELECT RVNU_RUN_ID, TRANS_NO, (SUM(ACT_OWNR_ADJ_AMT) + SUM(REDSTRB_EXMPT_ADJ_AMT)) AS Other		--- 6/28 - Added REDSTRB_EXMPT_ADJ_AMT
			FROM QAS.RTRN_RD_OWNR_LVL_ADJ_PSUM_HIST
			GROUP BY RVNU_RUN_ID, TRANS_NO
		) adj ON
			rh.RVNU_RUN_ID = adj.RVNU_RUN_ID
			AND rh.TRANS_NO = adj.TRANS_NO
		LEFT JOIN ParsleyDW.dbo.DimDate dda ON
			dbo.ConvertIntegersToDate(1, MONTH(rh.ACCTG_MTH), YEAR(rh.ACCTG_MTH)) = dda.Date
		LEFT JOIN ParsleyDW.dbo.DimCompany dc ON
			rh.BUS_UNIT_CD = dc.BKCompanyKey
		LEFT JOIN ParsleyDW.dbo.DimDOI ddoi ON
			rh.PROP_NO = ddoi.BKPropertyNumber
			AND rh.DO_MAJ_PROD_CD = ddoi.BKProductCode
			AND rh.DO_TYPE_CD = ddoi.BKDOTypeCode
			AND rh.TIER = ddoi.BKTier
		LEFT JOIN ParsleyDW.dbo.DimProduct dpr ON
			rh.PROD_CD = dpr.BKProductCode
		LEFT JOIN ParsleyDW.dbo.DimDate ddp ON
			rh.PRDN_DT = ddp.Date
		LEFT JOIN ParsleyDW.dbo.DimProperty dp ON
			rh.PROP_NO = dp.BKPropertyKey
		LEFT JOIN ParsleyDW.dbo.DimPurchaser dpu ON
			rh.RMITR_NO = dpu.BKPurchaserKey
		LEFT JOIN ParsleyDW.dbo.DimUser du ON 
			rh.UPDT_USER = du.BKUserKey
		LEFT JOIN [ParsleyDW].dbo.DimWellAge wa ON
			wa.AgeInMonths = DATEDIFF(mm, dp.FirstProductionDate, ddp.Date)
		OUTER APPLY 
		(
			SELECT TOP 1 ISNULL(pnm.NewPropertyNumber, p.PropertyNumber) AS PropertyNumber
				, c.MerrickID 
			FROM PropertyWellInfoTb p 
				JOIN CompletionTb c ON 
					c.PropertyWellID = p.PropertyWellMerrickID
				LEFT JOIN [dbo].[PropertyNumberMapping] pnm ON
					pnm.OldPropertyNumber =p.PropertyNumber
			WHERE ISNULL(pnm.NewPropertyNumber, p.PropertyNumber) = rh.PROP_NO
			ORDER BY p.PropertyWellMerrickID 
		) c 
		OUTER APPLY
		(
			SELECT TOP 1 IntegerValue 
			FROM SetupValueTb sp 
			WHERE sp.ObjectType = 1 AND 
				sp.SetupItem = 6 AND -- PowerMethod
				sp.ObjectId = c.MerrickID 
				AND ddp.Date BETWEEN sp.StartDate AND sp.EndDate
			ORDER BY sp.StartDate DESC
		) sp
		OUTER APPLY
		(
			SELECT TOP 1 IntegerValue
			FROM SetupValueTb ss 
			WHERE ss.ObjectType = 1 AND 
				ss.SetupItem = 10 AND -- SWDMethod
				ss.ObjectId = c.MerrickID AND 
				ddp.Date BETWEEN ss.StartDate AND ss.EndDate
			ORDER BY ss.StartDate
		) ss
		LEFT JOIN DDATb ddat ON 
			ddat.DDAFieldMerrickID = sp.IntegerValue 
		LEFT JOIN FieldGroupTb f ON 
			f.FieldGroupMerrickID = ss.IntegerValue 
		LEFT JOIN ParsleyDW.dbo.DimSWDMethod sm ON 
			sm.BKFieldGroupMerrickID = f.FieldGroupMerrickID
		LEFT JOIN ParsleyDW.dbo.DimPowerMethod pm ON 
			pm.BKDDAFieldMerrickID = ddat.DDAFieldMerrickID
		OUTER APPLY
		(
			SELECT TOP 1 p.MerrickID AS BKPersonnelID
			FROM SetupValueTb sp 
				INNER JOIN ProCount.PersonnelTb p ON
					sp.IntegerValue = p.MerrickID
			WHERE sp.ObjectType = 1 AND 
				sp.SetupItem = 11 AND -- Foreman
				sp.ObjectId = c.MerrickID AND 
				ddp.Date BETWEEN sp.StartDate AND sp.EndDate
			ORDER BY sp.StartDate DESC
		) fm  
		LEFT JOIN ParsleyDW.dbo.DimForeman foreman ON 
			foreman.BKPersonnelID = fm.BKPersonnelID
		OUTER APPLY 
		(
			SELECT TOP 1 p.MerrickID AS BKPersonnelID
			FROM SetupValueTb sp 
				INNER JOIN ProCount.PersonnelTb p ON
					sp.IntegerValue = p.MerrickID
			WHERE sp.ObjectType = 1 AND 
				sp.SetupItem = 20 AND -- Pumper
				sp.ObjectId = c.MerrickID AND 
				ddp.Date BETWEEN sp.StartDate AND sp.EndDate
		) pp  
		LEFT JOIN ParsleyDW.dbo.DimPumper pumper ON
			pumper.BKPersonnelID = pp.BKPersonnelID
		OUTER APPLY
		(
		  SELECT TOP 1 IntegerValue AS BKBusinessEntityMerrickID
		  FROM SetupValueTb sp 
			INNER JOIN ConnectTb ct ON
			  ct.DownstreamType = 3
			  AND ct.UpstreamType = 1
			  AND sp.ObjectID = ct.DownstreamID
			  AND ct.UpstreamID = c.MerrickID
		  WHERE sp.ObjectType = 3 AND 
			sp.SetupItem = 64 AND -- Hauler
			ddp.Date BETWEEN sp.StartDate AND sp.EndDate
		  ORDER BY sp.StartDate DESC 
		) otp
		LEFT JOIN ParsleyDW.dbo.DimOilTransporter ot ON
		  otp.BKBusinessEntityMerrickID = ot.BKBusinessEntityMerrickID            
			OUTER APPLY
		(
		  SELECT TOP 1 IntegerValue AS BKGroupMerrickID
		  FROM SetupValueTb sp 
			INNER JOIN ProCount.GroupTb g ON
			  sp.IntegerValue = g.GroupMerrickID
		  WHERE sp.ObjectType = 1 AND
			sp.SetupItem = 14 AND -- Group (SWD)
			sp.ObjectId = c.MerrickID AND
			ddp.Date BETWEEN sp.StartDate AND sp.EndDate
		  ORDER BY sp.StartDate DESC 
		) swdp
		LEFT JOIN ParsleyDW.dbo.DimSWD swd ON
		  swdp.BKGroupMerrickID = swd.BKGroupMerrickID

	---------------------------------------------------------------------------
	-- Create records inserted Audit Event.
	---------------------------------------------------------------------------
	EXEC ETLControl.ETL.usp_CreateAuditEvent @ETLGroupID , @ETLGroupJobID, @ETLGroupExecutionID, 210
	, @@ROWCOUNT, @Description, @AuditEventID OUTPUT

	-- Insert records from Archived tables.
	INSERT INTO ParsleyDW.dbo.FactRevenueGrossHistory
	(	
		AuditEventID,
		BKBchSelSeqNo,
		BKIntrnlTransSub,
		BKRvnuRunID,
		AccountingDateID,
		CompanyID,
		DOIID,
		ForemanID,
		OilTransporterID,
		PowerMethodID,
		ProductID,
		ProductionDateID,
		PropertyID,
		PumperID,
		PurchaserID,
		SWDID,
		SWDMethodID,
		UserID,
		WellAgeID,
		BOE,
		Gross,
		MCFE,
		Net,
		Other,
		Price,
		Tax,
		Volume
	)
	SELECT 
		-3 AS AuditEventID,
		rfh.BCH_SEL_SEQ_NO,
		rfh.INTRNL_TRANS_SUB,
		rfh.RVNU_RUN_ID,
		COALESCE(dda.DateID, -1) AS AccountingDateID,
		COALESCE(dc.CompanyID, -1) AS CompanyID,
		COALESCE(ddoi.DOIID, -1) AS DOIID,
		COALESCE(foreman.ForemanID, -1) AS ForemanID,
		COALESCE(ot.OilTransporterID, -1) AS OilTransporterID,
		COALESCE(pm.PowerMethodID, -1) AS PowerMethodID,
		COALESCE(dpr.ProductID, -1) AS ProductID,
		COALESCE(ddp.DateID, -1) AS ProductionDateID,
		COALESCE(dp.PropertyID, -1) AS PropertyID,
		COALESCE(pumper.PumperID, -1) AS PumperID,
		COALESCE(dpu.PurchaserID, -1) AS PurchaserID,
		COALESCE(swd.SWDID, -1) AS SWDID,
		COALESCE(sm.SWDMethodID, -1) AS SWDMethodID,
		COALESCE(du.UserID, -1) AS UserID,
		CASE WHEN DATEDIFF(mm, dp.FirstProductionDate, ddp.Date) < 0 AND dp.FirstProductionDate <> '1900-01-01' AND ddp.Date <> '1900-01-01' 
			THEN -2 
			ELSE COALESCE(wa.WellAgeID, -1) 
		END AS WellAgeID,
		CASE rfh.CTR_UOM_CD
			WHEN 'BBL' THEN rfh.ACT_LSE_GRS_VOL
			WHEN 'MCF' THEN rfh.ACT_LSE_GRS_VOL / 6.0
			ELSE 0
		END AS BOE,
		rfh.ACT_LSE_GRS_AMT AS Gross,
		CASE rfh.CTR_UOM_CD
			WHEN 'BBL' THEN rfh.ACT_LSE_GRS_VOL * 6.0
			WHEN 'MCF' THEN rfh.ACT_LSE_GRS_VOL
			ELSE 0
		END AS MCFE,
		(rfh.ACT_LSE_GRS_AMT - ISNULL(tax.Tax, 0) - ISNULL(adj.Other, 0)) AS Net,
		adj.Other AS Other,
		CASE WHEN (rfh.ACT_LSE_GRS_VOL IS NULL OR rfh.ACT_LSE_GRS_VOL = 0) THEN NULL ELSE (rfh.ACT_LSE_GRS_AMT / rfh.ACT_LSE_GRS_VOL) END AS Price,
		tax.Tax AS Tax,
		rfh.ACT_LSE_GRS_VOL AS Volume
	FROM QAS.ARCV_RTRN_FINAL_HIST rfh
		INNER JOIN QAS.ARCV_RTRN_BCH_SEL_HIST rbsh ON 
			rfh.BCH_SEL_SEQ_NO = rbsh.BCH_SEL_SEQ_NO 
			AND rfh.RVNU_RUN_ID = rbsh.RVNU_RUN_ID		
		LEFT JOIN
		(
			SELECT BCH_SEL_SEQ_NO, INTRNL_TRANS_SUB, RVNU_RUN_ID, SUM(ACT_LSE_TAX_AMT) AS Tax
			FROM QAS.ARCV_RTRN_FINAL_HIST_TAX
			GROUP BY BCH_SEL_SEQ_NO, INTRNL_TRANS_SUB, RVNU_RUN_ID
		) tax ON
			rfh.BCH_SEL_SEQ_NO = tax.BCH_SEL_SEQ_NO
			AND rfh.INTRNL_TRANS_SUB = tax.INTRNL_TRANS_SUB
			AND rfh.RVNU_RUN_ID = tax.RVNU_RUN_ID
		LEFT JOIN
		(
			SELECT BCH_SEL_SEQ_NO, INTRNL_TRANS_SUB, RVNU_RUN_ID, SUM(ACT_LSE_ADJ_AMT) AS Other
			FROM QAS.ARCV_RTRN_FINAL_HIST_ADJ
			GROUP BY BCH_SEL_SEQ_NO, INTRNL_TRANS_SUB, RVNU_RUN_ID
		) adj ON
			rfh.BCH_SEL_SEQ_NO = adj.BCH_SEL_SEQ_NO
			AND rfh.INTRNL_TRANS_SUB = adj.INTRNL_TRANS_SUB
			AND rfh.RVNU_RUN_ID = adj.RVNU_RUN_ID
		LEFT JOIN ParsleyDW.dbo.DimDate dda ON
			dbo.ConvertIntegersToDate(1, MONTH(rbsh.ACCTG_MTH), YEAR(rbsh.ACCTG_MTH)) = dda.Date
		LEFT JOIN ParsleyDW.dbo.DimCompany dc ON
			rbsh.BUS_UNIT_CD = dc.BKCompanyKey
		LEFT JOIN ParsleyDW.dbo.DimDOI ddoi ON
			rbsh.PROP_NO = ddoi.BKPropertyNumber
			AND rbsh.DO_MAJ_PROD_CD = ddoi.BKProductCode
			AND rbsh.DO_TYPE_CD = ddoi.BKDOTypeCode
			AND rbsh.TIER = ddoi.BKTier
		LEFT JOIN ParsleyDW.dbo.DimProduct dpr ON
			rfh.PROD_CD = dpr.BKProductCode
		LEFT JOIN ParsleyDW.dbo.DimDate ddp ON
			rbsh.PRDN_DT = ddp.Date
		LEFT JOIN ParsleyDW.dbo.DimProperty dp ON
			rbsh.PROP_NO = dp.BKPropertyKey
		LEFT JOIN ParsleyDW.dbo.DimPurchaser dpu ON
			rbsh.RMITR_NO = dpu.BKPurchaserKey
		LEFT JOIN ParsleyDW.dbo.DimUser du ON 
			rfh.UPDT_USER = du.BKUserKey
		LEFT JOIN [ParsleyDW].dbo.DimWellAge wa ON
			wa.AgeInMonths = DATEDIFF(mm, dp.FirstProductionDate, ddp.Date)
		OUTER APPLY 
		(
			SELECT TOP 1	ISNULL(pnm.NewPropertyNumber, p.PropertyNumber) AS PropertyNumber
							, c.MerrickID 
			FROM PropertyWellInfoTb p 
				JOIN CompletionTb c ON 
					c.PropertyWellID = p.PropertyWellMerrickID
				LEFT JOIN [dbo].[PropertyNumberMapping] pnm ON
					pnm.OldPropertyNumber =p.PropertyNumber
			WHERE ISNULL(pnm.NewPropertyNumber, p.PropertyNumber) = rbsh.PROP_NO
			ORDER BY p.PropertyWellMerrickID 
		) c 
		OUTER APPLY
		(
			SELECT TOP 1 IntegerValue 
			FROM SetupValueTb sp 
			WHERE sp.ObjectType = 1 AND 
				sp.SetupItem = 6 AND -- PowerMethod
				sp.ObjectId = c.MerrickID 
				AND ddp.Date BETWEEN sp.StartDate AND sp.EndDate
			ORDER BY sp.StartDate DESC
		) sp
		OUTER APPLY
		(
			SELECT TOP 1 IntegerValue
			FROM SetupValueTb ss 
			WHERE ss.ObjectType = 1 AND 
				ss.SetupItem = 10 AND -- SWDMethod
				ss.ObjectId = c.MerrickID AND 
				ddp.Date BETWEEN ss.StartDate AND ss.EndDate
			ORDER BY ss.StartDate
		) ss
		LEFT JOIN DDATb ddat ON 
			ddat.DDAFieldMerrickID = sp.IntegerValue 
		LEFT JOIN FieldGroupTb f ON 
			f.FieldGroupMerrickID = ss.IntegerValue 
		LEFT JOIN ParsleyDW.dbo.DimSWDMethod sm ON 
			sm.BKFieldGroupMerrickID = f.FieldGroupMerrickID
		LEFT JOIN ParsleyDW.dbo.DimPowerMethod pm ON 
			pm.BKDDAFieldMerrickID = ddat.DDAFieldMerrickID
		OUTER APPLY
		(
			SELECT TOP 1 p.MerrickID AS BKPersonnelID
			FROM SetupValueTb sp 
				INNER JOIN ProCount.PersonnelTb p ON
					sp.IntegerValue = p.MerrickID
			WHERE sp.ObjectType = 1 AND 
				sp.SetupItem = 11 AND -- Foreman
				sp.ObjectId = c.MerrickID AND 
				ddp.Date BETWEEN sp.StartDate AND sp.EndDate
			ORDER BY sp.StartDate DESC
		) fm  
		LEFT JOIN ParsleyDW.dbo.DimForeman foreman ON 
			foreman.BKPersonnelID = fm.BKPersonnelID
		OUTER APPLY 
		(
			SELECT TOP 1 p.MerrickID AS BKPersonnelID
			FROM SetupValueTb sp 
				INNER JOIN ProCount.PersonnelTb p ON
					sp.IntegerValue = p.MerrickID
			WHERE sp.ObjectType = 1 AND 
				sp.SetupItem = 20 AND -- Pumper
				sp.ObjectId = c.MerrickID AND 
				ddp.Date BETWEEN sp.StartDate AND sp.EndDate
		) pp  
		LEFT JOIN ParsleyDW.dbo.DimPumper pumper ON
			pumper.BKPersonnelID = pp.BKPersonnelID
		OUTER APPLY
		(
		  SELECT TOP 1 IntegerValue AS BKBusinessEntityMerrickID
		  FROM SetupValueTb sp 
			INNER JOIN ConnectTb ct ON
			  ct.DownstreamType = 3
			  AND ct.UpstreamType = 1
			  AND sp.ObjectID = ct.DownstreamID
			  AND ct.UpstreamID = c.MerrickID
		  WHERE sp.ObjectType = 3 AND 
			sp.SetupItem = 64 AND -- Hauler
			ddp.Date BETWEEN sp.StartDate AND sp.EndDate
		  ORDER BY sp.StartDate DESC 
		) otp
		LEFT JOIN ParsleyDW.dbo.DimOilTransporter ot ON
		  otp.BKBusinessEntityMerrickID = ot.BKBusinessEntityMerrickID            
			OUTER APPLY
		(
		  SELECT TOP 1 IntegerValue AS BKGroupMerrickID
		  FROM SetupValueTb sp 
			INNER JOIN ProCount.GroupTb g ON
			  sp.IntegerValue = g.GroupMerrickID
		  WHERE sp.ObjectType = 1 AND
			sp.SetupItem = 14 AND -- Group (SWD)
			sp.ObjectId = c.MerrickID AND
			ddp.Date BETWEEN sp.StartDate AND sp.EndDate
		  ORDER BY sp.StartDate DESC 
		) swdp
		LEFT JOIN ParsleyDW.dbo.DimSWD swd ON
		  swdp.BKGroupMerrickID = swd.BKGroupMerrickID

	---------------------------------------------------------------------------
	-- Create records inserted Audit Event.
	---------------------------------------------------------------------------
	EXEC ETLControl.ETL.usp_CreateAuditEvent @ETLGroupID , @ETLGroupJobID, @ETLGroupExecutionID, 210
	, @@ROWCOUNT, @Description, @AuditEventID OUTPUT

	---------------------------------------------------------------------------
	-- Create completed Audit Event.
	---------------------------------------------------------------------------
	SET @Description = 'Completed ' + @TargetTable + ' Load'
	EXEC ETLControl.ETL.usp_CreateAuditEvent @ETLGroupID , @ETLGroupJobID, @ETLGroupExecutionID, 244
	, NULL, @Description, @AuditEventID OUTPUT

END











GO



USE [Welland_Sandbox]
GO

/****** Object:  StoredProcedure [dbo].[uspREV_SUSPENSE_UPLOAD]    Script Date: 1/4/2021 1:08:43 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- ============================================================================
-- Author: Heath Cameron
-- Create date: 12/5/2019
--
--
-- Sample invocation:
-- Exec [dbo].[uspREV_SUSPENSE_UPLOAD] 0, 0, 0, 0 
-- ============================================================================

CREATE PROCEDURE [dbo].[uspREV_SUSPENSE_UPLOAD] AS

BEGIN
	---------------------------------------------------------------------------
	-- Empty Table that holds cleaned and transformed conversion data
	---------------------------------------------------------------------------

TRUNCATE TABLE [STG].[JSTG_MANL_JE_DETAIL]

	---------------------------------------------------------------------------
	-- Preemptive dropping of temp tables used 
	---------------------------------------------------------------------------

IF object_id(N'tempdb..#OWNERTEMP') IS NOT NULL DROP TABLE #OWNERTEMP
IF object_id(N'tempdb..#LSE_ADJ_AMT') IS NOT NULL DROP TABLE #LSE_ADJ_AMT

	---------------------------------------------------------------------------
	-- Consolidating and cleaning of *WELL* level data. Inserted into temp table
	---------------------------------------------------------------------------

SELECT G.U2_ID,  
MAX(G.GROSS_WO_RT) AS GROSS_VAL, 
CASE WHEN MAX(PRODUCT) = 'NGL' THEN MAX(QUANTITY)/42 ELSE max(QUANTITY) END as GROSS_QTY,
MAX(G.PROPERTY_ID) AS PROPERTY_ID,
MAX(PRODUCT) AS PRODUCT, 
SUM(CASE WHEN COMPONENT NOT IN ('SEV','SEV1', 'TXC','TXC1') THEN COALESCE(COMPONENT_AMOUNT,CALC_COMPT_AMOUNT,0) ELSE 0 END) AS LSE_ADJ_AMT,
(SUM(CASE WHEN COMPONENT IN ('SEV','SEV1') THEN COALESCE(COMPONENT_AMOUNT,0) ELSE 0 END))  AS LSE_PR_TAX_AMT,
SUM(CASE WHEN COMPONENT IN ('TXC','TXC1') THEN COALESCE(COMPONENT_AMOUNT,0)  ELSE 0 END) AS LSE_VOL_TAX_AMT,
SUM(CASE WHEN COMPONENT IN ('SEV','SEV1', 'TXC','TXC1') THEN COALESCE(COMPONENT_AMOUNT,0) ELSE 0 END) AS LST_TOT_TAX_AMT,
SUM(COALESCE(WE_REMIT_AMT,0)) as REMITTED_TAXES
,MAX(G.AMOUNT_RECEIVED) AS PROP_NET_AMT
, MAX(G.AMOUNT_DUE) AS PROP_AMT_DUE
, MAX(CALC_REC_INT_AMT) AS CALC_REC_INT_AMT
INTO #LSE_ADJ_AMT 
from WELLAND_EXPORT_PROD.DBO.GSL G
left JOIN WELLAND_EXPORT_PROD.DBO.GSL_ASSOC1 S
ON G.u2_id = S.U2_ID
GROUP BY G.U2_ID

	---------------------------------------------------------------------------
	-- Consolidating and cleaning of *OWNER* level data. Inserted into temp table
	---------------------------------------------------------------------------

SELECT  Welland_Sandbox.dbo.UFN_SEPARATES_COLUMNS(DSL._ID, 2,'*') AS BA_NO, DSL.INTEREST,  S.PAR_PAY_CD, 
S.PAR_SUSP_RSN,WELLAND_SANDBOX.DBO.UFN_SEPARATES_COLUMNS(WELL_ID, 2, '*') AS  WELL_ID, P.PAR_PROD, DSL.PRODUCT, 
CASE WHEN DSL.PRODUCT = 'NGL' THEN DSL.QUANTITY/42 ELSE DSL.QUANTITY END AS QUANTITY, IT.PAR_INT_TYPE, GROSS,
SUM(CASE WHEN PAR_ADJ_OIL <> '10' THEN COALESCE(DSLC.COMP_AMT,0) ELSE 0 END) AS TOT_TAX_GRS_AMT,
SUM(CASE WHEN PAR_ADJ_OIL = '10' THEN COALESCE(DSLC.COMP_AMT,0) ELSE 0 END) AS TOT_ADJ_GRS_AMT,
SUM(CASE WHEN PAR_ADJ_OIL = 'PR' THEN COALESCE(DSLC.COMP_AMT,0) ELSE 0 END) AS PR_TAX,
SUM(CASE WHEN PAR_ADJ_OIL NOT IN ('PR','10') THEN COALESCE(DSLC.COMP_AMT,0) ELSE 0 END) AS VOL_TAX,
SUM(CASE WHEN PAR_ADJ_OIL = '10' THEN COALESCE(DSLC.COMP_AMT,0) ELSE 0 END) AS ADJ,
COALESCE(GROSS,0) - SUM(COALESCE(DSLC.COMP_AMT,0)) AS OWNR_NET,
DSL.GROSS_SALES_REF, DSL.SALE_DATE, DSL.u2_id
INTO #OWNERTEMP
FROM WELLAND_EXPORT_PROD.DBO.DETAIL_SALES_LEDGER DSL
LEFT JOIN WELLAND_EXPORT_PROD.DBO.DETAIL_SALES_LEDGER_COMP DSLC ON DSL.U2_ID = DSLC.U2_ID
LEFT JOIN MAP.REV_ADJ_XREF X
ON X.JAG_ADJ = DSLC.COMP
LEFT JOIN MAP.REV_SUSP_RSN S
ON S.JAG_PAY_CODE = DSL.PAY_CODE_NP
LEFT JOIN MAP.REV_PROD_XREF P
ON DSL.PRODUCT = JAG_PROD
LEFT JOIN WELLAND_SANDBOX.MAP.INT_TYPE_XREF IT 
ON IT.JAG_INT_TYPE = DSL.TYPE_INT
WHERE LEN(CHECK_NO) < 1 AND DSL.PAY_CODE_NP NOT LIKE 'X%' AND DSL.PAY_CODE_NP <> 'VOC'
GROUP BY  DSL.u2_id, WELL_ID, DSL.PRODUCT, Welland_Sandbox.dbo.UFN_SEPARATES_COLUMNS(DSL._ID, 2,'*'), 
P.PAR_PROD, S.PAR_PAY_CD, S.PAR_SUSP_RSN, GROSS, DSL.QUANTITY, DSL.INTEREST, DSL.TYPE_INT, DSL.GROSS_SALES_REF, DSL.SALE_DATE, IT.PAR_INT_TYPE

	---------------------------------------------------------------------------
	-- Transforming and inserting data into conversion journal entry table. Using joins on master data cross references created in previous conversion rounds. 
	---------------------------------------------------------------------------

INSERT INTO [STG].[JSTG_MANL_JE_DETAIL]
           ([PROCESS_QUEUE_ID]
           ,[PROCESS_STEP_QUEUE_ID]
           ,[MANL_JE_NO]
           ,[BCH_NO]
           ,[OPER_BUS_SEG_CD]
           ,[ORIG_BUS_UNIT_CD]
           ,[ACCT_NO]
           ,[TRANS_AMT]
           ,[TRANS_QTY]
           ,[BUS_SEG_STD_TRANS_VOL]
           ,[TRANS_VAL_AMT]
           ,[TRANS_DTL_EXPLANATION]
           ,[GRS_AMT]
           ,[GRS_QTY]
           ,[MAJ_PROD_CD]
           ,[PROD_CD]
           ,[DISP_CD]
           ,[CHK_NO]
           ,[VOL_CLASS_CD]
           ,[VOL_SRC_CD]
           ,[VOL_FREQ_CD]
           ,[BTU_FACT]
           ,[BTU_BASIS_CD]
           ,[STD_PRES]
           ,[STD_GRV]
           ,[OWNR_BA_NO]
           ,[OWNR_BA_SUB]
           ,[OWNR_INT_TYPE_CD]
           ,[OWNR_INT_TYPE_SEQ_NO]
           ,[OWNR_INT_DEC]
           ,[CTR_PTY_NO]
           ,[CTR_PTY_SUB]
           ,[PDCR_BA_NO]
           ,[PDCR_BA_SUB]
           ,[CHK_DT]
           ,[UOM_CD]
           ,[CASH_RCPT_DT]
           ,[SUSP_RSN_CD]
           ,[CTR_NO]
           ,[CTR_TYPE_CD]
           ,[PROP_NO]
           ,[DO_TYPE_CD]
           ,[DO_MAJ_PROD_CD]
           ,[TIER]
           ,[WELL_NO]
           ,[COMPL_NO]
           ,[ORIG_MP_NO]
           ,[ORIG_MP_TYPE]
           ,[INV_NO]
           ,[INV_DT]
           ,[PAY_CD]
           ,[SALES_MP_NO]
           ,[PRDN_DT]
           ,[ST_CD]
           ,[CTRY_CD]
           ,[TAX_AUTH_BA_NO]
           ,[TAX_AUTH_BA_SUB]
           ,[OWNR_BAL_DEC]
           ,[RMITR_NO]
           ,[RMITR_SUB]
           ,[RMITR_CHK_NO]
           ,[SVC_CTR_NO]
           ,[OWNR_PVR_VOL]
           ,[GRS_PVR_VOL]
           ,[MMBTU_OWNR_VOL]
           ,[MMBTU_GRS_VOL]
           ,[DOWNLOAD_GL_INTFC_FL]
           ,[TOT_SEV_TAX_NET_AMT]
           ,[TOT_SEV_TAX_LSE_GRS_AMT]
           ,[TOT_ADJ_CTGY_NET_AMT]
           ,[TOT_ADJ_CTGY_LSE_GRS_AMT]
           ,[TOT_SEV_TAX_REIMB_NET_AMT]
           ,[TOT_SEV_TAX_REIMB_LSE_GRS_AMT]
           ,[SEV_TAX_TYPE_CD_1]
           ,[TAX_NET_AMT_1]
           ,[TAX_LSE_GRS_AMT_1]
           ,[LSE_GRS_TAXABLE_AMT_1]
           ,[REIMB_NET_AMT_1]
           ,[REIMB_LSE_GRS_AMT_1]
           ,[SEV_TAX_TYPE_CD_2]
           ,[TAX_NET_AMT_2]
           ,[TAX_LSE_GRS_AMT_2]
           ,[LSE_GRS_TAXABLE_AMT_2]
           ,[REIMB_NET_AMT_2]
           ,[REIMB_LSE_GRS_AMT_2]
           ,[ADJ_CTGY_CD_1]
           ,[DDUC_REIMB_CD_1]
           ,[ADJ_NET_AMT_1]
           ,[ADJ_LSE_GRS_AMT_1]
           ,[CUR_CD]
           ,[UPDT_USER]
           ,[UPDT_DT]
           ,[ACCTG_MTH])
SELECT
           null AS [PROCESS_QUEUE_ID], --[PROCESS_QUEUE_ID]
           null AS [PROCESS_STEP_QUEUE_ID], --[PROCESS_STEP_QUEUE_ID]
           NULL AS [MANL_JE_NO], --[MANL_JE_NO],
           --ROUND(ROW_NUMBER() OVER(ORDER BY PAR_PAY_CD, OWNR_NET),-1)  AS [BCH_NO], --[BCH_NO],
		   CASE WHEN PAR_PAY_CD = '1' THEN 1 ELSE 2 END AS [BCH_NO],
           'PAR' AS [OPER_BUS_SEG_CD],--[OPER_BUS_SEG_CD]
           '001' AS [ORIG_BUS_UNIT_CD],--[ORIG_BUS_UNIT_CD]
           CASE WHEN PAR_PAY_CD = '1' THEN '0205.0001.00' ELSE '0205.0005.00' END AS [ACCT_NO] ,--[ACCT_NO]
           -OL.OWNR_NET AS  [TRANS_AMT] ,--[TRANS_AMT]
           -OL.QUANTITY AS [TRANS_QTY],--[TRANS_QTY]
           NULL AS [BUS_SEG_STD_TRANS_VOL],--[BUS_SEG_STD_TRANS_VOL]
           COALESCE(-OL.GROSS, 0)  AS [TRANS_VAL_AMT],--,[TRANS_VAL_AMT]
           NULL AS [TRANS_DTL_EXPLANATION],--[TRANS_DTL_EXPLANATION]
           -COALESCE(LSE.GROSS_VAL,0) AS GRS_AMT, --GRS_AMT
           -COALESCE(LSE.GROSS_QTY,0) AS GRS_QTY, --GRS_QTY
           CASE WHEN PAR_PROD IS NULL THEN '100' ELSE CONCAT(LEFT(PAR_PROD,1),'00') END AS [MAJ_PROD_CD],--,[MAJ_PROD_CD]
           CASE WHEN PAR_PROD IS NULL THEN '100' ELSE PAR_PROD END AS [PROD_CD],--[PROD_CD]
           '03' AS [DISP_CD],--[DISP_CD]
           NULL AS [CHK_NO],--[CHK_NO]
           'A' AS [VOL_CLASS_CD], --[VOL_CLASS_CD]
           NULL AS [VOL_SRC_CD],--[VOL_SRC_CD]
           NULL AS [VOL_FREQ_CD],--[VOL_FREQ_CD]
           CASE WHEN OL.PRODUCT = 'G' THEN 1 ELSE NULL END AS [BTU_FACT],--[BTU_FACT]
           CASE WHEN OL.PRODUCT = 'G' THEN 'W' ELSE NULL END AS [BTU_BASIS_CD],--[BTU_BASIS_CD]
           CASE WHEN OL.PRODUCT = 'G' THEN '14.65' ELSE NULL END AS [STD_PRES],--[STD_PRES]
           CASE WHEN OL.PRODUCT = 'O' THEN '40' ELSE NULL END AS [STD_GRV],--[STD_GRV]
           Q_REV_BA_NO AS [OWNR_BA_NO],--[OWNR_BA_NO]
           Q_REV_BA_SUF AS [OWNR_BA_SUB],--[OWNR_BA_SUB]
           PAR_INT_TYPE AS [OWNR_INT_TYPE_CD],--[OWNR_INT_TYPE_CD]
           '1' AS [OWNR_INT_TYPE_SEQ_NO],--[OWNR_INT_TYPE_SEQ_NO]
           INTEREST AS [OWNR_INT_DEC],--[OWNR_INT_DEC]
           NULL AS [CTR_PTY_NO],--[CTR_PTY_NO]
           NULL AS [CTR_PTY_SUB],--[CTR_PTY_SUB]
           NULL AS [PDCR_BA_NO],--[PDCR_BA_NO]
           NULL AS [PDCR_BA_SUB],--[PDCR_BA_SUB]
           NULL AS [CHK_DT],--[CHK_DT]
           CASE WHEN PAR_PROD IN ('100', '300', '400') THEN 'BBL' ELSE 'MCF' END AS UOM_CD, --UOM_CD
           NULL,--[CASH_RCPT_DT]
           PAR_SUSP_RSN,--[SUSP_RSN_CD]
           NULL,--[CTR_NO]
           NULL,--[CTR_TYPE_CD]
           P.PAR_PROP_NO, --PROP_NO
           'REV' AS DO_TYPE_CD,--[DO_TYPE_CD]
           'ALL' AS DO_MAJ_PROD_CD, --DO_MAJ_PROD_CD
           CASE WHEN TIER.JAG_PROP_NO IS NULL THEN 1 ELSE 50 END AS TIER,--[TIER] TBD
           P.PAR_PROP_NO AS [WELL_NO],--[WELL_NO]
           '1' AS [COMPL_NO],--[COMPL_NO]
           NULL AS [ORIG_MP_NO],--[ORIG_MP_NO]
           NULL AS [ORIG_MP_TYPE], --[ORIG_MP_TYPE]
           NULL AS [INV_NO], --,[INV_NO]
           NULL AS [INV_DT], --,[INV_DT]
           PAR_PAY_CD AS [PAY_CD], --[PAY_CD]
           NULL AS [SALES_MP_NO], --,[SALES_MP_NO]
           DATEADD(month, DATEDIFF(month, 0, OL.SALE_DATE), 0) AS [PRDN_DT] ,--[PRDN_DT]
           '42' AS ST_CD, --ST_CD
			'US' AS CTRY_CD, --CTRY_CD,
           NULL AS [TAX_AUTH_BA_NO],--[TAX_AUTH_BA_NO]
           NULL AS [TAX_AUTH_BA_SUB],--,[TAX_AUTH_BA_SUB]
           INTEREST,--[OWNR_BAL_DEC]
           NULL AS [RMITR_NO],--[RMITR_NO]
           NULL AS [RMITR_SUB],--[RMITR_SUB]
           NULL AS [RMITR_CHK_NO],--,[RMITR_CHK_NO]
           NULL AS [SVC_CTR_NO],--,[SVC_CTR_NO]
           NULL AS [OWNR_PVR_VOL],--,[OWNR_PVR_VOL]
           NULL AS [GRS_PVR_VOL],--,[GRS_PVR_VOL]
           NULL AS [MMBTU_OWNR_VOL],--[MMBTU_OWNR_VOL]
           NULL AS [MMBTU_GRS_VOL],--[MMBTU_GRS_VOL]
           'Y' AS [DOWNLOAD_GL_INTFC_FL],--[DOWNLOAD_GL_INTFC_FL]
            TOT_TAX_GRS_AMT AS TOT_SEV_TAX_NET_AMT,--[TOT_SEV_TAX_NET_AMT]
           COALESCE(LSE.LSE_PR_TAX_AMT + LSE.LSE_VOL_TAX_AMT,0) AS TOT_SEV_TAX_LSE_GRS_AMT, --TOT_SEV_TAX_LSE_GRS_AMT
           OL.ADJ AS TOT_ADJ_CTGY_NET_AMT, --TOT_ADJ_CTGY_NET_AMT
           COALESCE(LSE.LSE_ADJ_AMT,0) AS TOT_ADJ_CTGY_LSE_GRS_AMT, --TOT_ADJ_CTGY_LSE_GRS_AMT
           '0' AS [TOT_SEV_TAX_REIMB_NET_AMT],--[TOT_SEV_TAX_REIMB_NET_AMT]
           '0' AS [TOT_SEV_TAX_REIMB_LSE_GRS_AMT],--[TOT_SEV_TAX_REIMB_LSE_GRS_AMT]
           'PR' AS [SEV_TAX_TYPE_CD_1],--[SEV_TAX_TYPE_CD_1]
           OL.PR_TAX AS [TAX_NET_AMT_1],--[TAX_NET_AMT_1]
           0 AS [TAX_LSE_GRS_AMT_1],--[TAX_LSE_GRS_AMT_1]
           '0' AS [LSE_GRS_TAXABLE_AMT_1],--[LSE_GRS_TAXABLE_AMT_1]
           '0' AS [REIMB_NET_AMT_1],--[REIMB_NET_AMT_1]
           '0' AS [REIMB_LSE_GRS_AMT_1],--[REIMB_LSE_GRS_AMT_1]
           CASE WHEN PAR_PROD IN ('200','400') THEN 'FE' ELSE 'RG' END,--[SEV_TAX_TYPE_CD_2]
           VOL_TAX,--[TAX_NET_AMT_2]
           LSE_VOL_TAX_AMT AS [TAX_LSE_GRS_AMT_2],--[TAX_LSE_GRS_AMT_2]
           0 AS [LSE_GRS_TAXABLE_AMT_2],--[LSE_GRS_TAXABLE_AMT_2]
           '0' AS [REIMB_NET_AMT_2],--[REIMB_NET_AMT_2]
           '0' AS [REIMB_LSE_GRS_AMT_2],--[REIMB_LSE_GRS_AMT_2]
           '10' AS [ADJ_CTGY_CD_1],--[ADJ_CTGY_CD_1]
           'D' AS [DDUC_REIMB_CD_1],--[DDUC_REIMB_CD_1]
           ADJ AS [ADJ_NET_AMT_1],--[ADJ_NET_AMT_1]
           LSE_ADJ_AMT AS [ADJ_LSE_GRS_AMT_1],--[ADJ_LSE_GRS_AMT_1]
           'USD' AS [CUR_CD],--[CUR_CD]
           'JAG_CONV' AS [UPDT_USER],--[UPDT_USER]
           CONVERT(SMALLDATETIME, Getdate()) AS [UPDT_DT],--[UPDT_DT]
           '3/1/2020' AS [ACCTG_MTH]--[ACCTG_MTH]
FROM #OWNERTEMP OL
LEFT JOIN Welland_Sandbox.MAP.BA_XREF BA ON BA.JAG_BA_NO = OL.BA_NO
LEFT JOIN WELLAND_SANDBOX.QAS.SCTRL_BA_ENTITY BAE ON BAE.BA_NO = Q_REV_BA_NO
LEFT JOIN WELLAND_SANDBOX.QAS.SCTRL_BA_ADDRESS BAA ON BAA.BA_NO = Q_REV_BA_NO AND BAA.BA_SUF = Q_REV_BA_SUF
LEFT JOIN WELLAND_SANDBOX.QAS.SCTRL_BA_TAX_ID BAT ON BAT.BA_NO = Q_REV_BA_NO
LEFT JOIN #LSE_ADJ_AMT LSE ON LSE.U2_ID = OL.GROSS_SALES_REF
LEFT JOIN WELLAND_SANDBOX.MAP.PROP_XREF P ON P.JAG_PROP_NO = OL.WELL_ID
LEFT JOIN WELLAND_SANDBOX.QAS.GONL_PROP GP ON GP.PROP_NO = P.PAR_PROP_NO
LEFT JOIN SUP.PROP_TIER_50 TIER ON TIER.JAG_PROP_NO = P.JAG_PROP_NO


	---------------------------------------------------------------------------
	-- Conversion journal entry batch must balance to zero. Creating summed offsetting entry to dummy owner that will be written off. 
	---------------------------------------------------------------------------


INSERT INTO [STG].[JSTG_MANL_JE_DETAIL]
           ([PROCESS_QUEUE_ID]
           ,[PROCESS_STEP_QUEUE_ID]
           ,[MANL_JE_NO]
           ,[BCH_NO]
           ,[OPER_BUS_SEG_CD]
           ,[ORIG_BUS_UNIT_CD]
           ,[ACCT_NO]
           ,[TRANS_AMT]
           ,[TRANS_QTY]
           ,[BUS_SEG_STD_TRANS_VOL]
           ,[TRANS_VAL_AMT]
           ,[TRANS_DTL_EXPLANATION]
           ,[GRS_AMT]
           ,[GRS_QTY]
           ,[MAJ_PROD_CD]
           ,[PROD_CD]
           ,[DISP_CD]
           ,[CHK_NO]
           ,[VOL_CLASS_CD]
           ,[VOL_SRC_CD]
           ,[VOL_FREQ_CD]
           ,[BTU_FACT]
           ,[BTU_BASIS_CD]
           ,[STD_PRES]
           ,[STD_GRV]
           ,[OWNR_BA_NO]
           ,[OWNR_BA_SUB]
           ,[OWNR_INT_TYPE_CD]
           ,[OWNR_INT_TYPE_SEQ_NO]
           ,[OWNR_INT_DEC]
           ,[CTR_PTY_NO]
           ,[CTR_PTY_SUB]
           ,[PDCR_BA_NO]
           ,[PDCR_BA_SUB]
           ,[CHK_DT]
           ,[UOM_CD]
           ,[CASH_RCPT_DT]
           ,[SUSP_RSN_CD]
           ,[CTR_NO]
           ,[CTR_TYPE_CD]
           ,[PROP_NO]
           ,[DO_TYPE_CD]
           ,[DO_MAJ_PROD_CD]
           ,[TIER]
           ,[WELL_NO]
           ,[COMPL_NO]
           ,[ORIG_MP_NO]
           ,[ORIG_MP_TYPE]
           ,[INV_NO]
           ,[INV_DT]
           ,[PAY_CD]
           ,[SALES_MP_NO]
           ,[PRDN_DT]
           ,[ST_CD]
           ,[CTRY_CD]
           ,[TAX_AUTH_BA_NO]
           ,[TAX_AUTH_BA_SUB]
           ,[OWNR_BAL_DEC]
           ,[RMITR_NO]
           ,[RMITR_SUB]
           ,[DOWNLOAD_GL_INTFC_FL]
           ,[CUR_CD]
           ,[UPDT_USER]
           ,[UPDT_DT]
           ,[ACCTG_MTH])
SELECT NULL AS [PROCESS_QUEUE_ID]
           , NULL AS [PROCESS_STEP_QUEUE_ID]
           ,  NULL AS [MANL_JE_NO]
           ,[BCH_NO]
           ,MAX([OPER_BUS_SEG_CD])
           ,MAX([ORIG_BUS_UNIT_CD])
           ,[ACCT_NO]
           ,-SUM([TRANS_AMT])
           ,-SUM([TRANS_QTY])
           ,MAX([BUS_SEG_STD_TRANS_VOL])
           ,-SUM([TRANS_AMT])
           ,NULL AS [TRANS_DTL_EXPLANATION]
           ,NULL AS [GRS_AMT]
           ,NULL AS [GRS_QTY]
           ,'100' AS [MAJ_PROD_CD]
           ,'100' AS [PROD_CD]
           ,'03' AS [DISP_CD]
           ,NULL AS [CHK_NO]
           ,NULL AS [VOL_CLASS_CD]
           ,NULL AS [VOL_SRC_CD]
           ,NULL AS [VOL_FREQ_CD]
           ,NULL AS [BTU_FACT]
           ,NULL AS [BTU_BASIS_CD]
           ,NULL AS [STD_PRES]
           , '40' AS [STD_GRV]
           ,'CONV' AS [OWNR_BA_NO]
           ,'1' AS [OWNR_BA_SUB]
           ,'RI' AS [OWNR_INT_TYPE_CD]
           ,'1' [OWNR_INT_TYPE_SEQ_NO]
           ,'1' AS [OWNR_INT_DEC]
           ,NULL AS [CTR_PTY_NO]
           ,NULL AS [CTR_PTY_SUB]
           ,NULL AS [PDCR_BA_NO]
           ,NULL AS [PDCR_BA_SUB]
           ,NULL AS [CHK_DT]
           ,'BBL' AS [UOM_CD]
           ,NULL AS [CASH_RCPT_DT]
           ,'N4' AS [SUSP_RSN_CD]
           ,NULL AS [CTR_NO]
           ,NULL AS [CTR_TYPE_CD]
           ,MAX([PROP_NO])
           ,'REV' AS [DO_TYPE_CD]
           ,'ALL' AS [DO_MAJ_PROD_CD]
           ,'1' AS [TIER]
           ,'2'
           ,'1' AS [COMPL_NO]
           ,NULL AS [ORIG_MP_NO]
           ,NULL AS [ORIG_MP_TYPE]
           ,NULL AS [INV_NO]
           ,NULL AS [INV_DT]
           ,'2' AS [PAY_CD]
           ,NULL AS [SALES_MP_NO]
           ,'3/1/2020' AS [PRDN_DT]
           ,'42' AS  [ST_CD]
           ,'US' AS [CTRY_CD]
           ,NULL AS [TAX_AUTH_BA_NO]
           ,NULL AS [TAX_AUTH_BA_SUB]
           ,'1' AS [OWNR_BAL_DEC]
           ,NULL AS [RMITR_NO]
           ,NULL AS [RMITR_SUB]
           ,NULL AS [DOWNLOAD_GL_INTFC_FL]
           ,'USD'
           ,'CONV'
           ,CONVERT(SMALLDATETIME, Getdate())
           ,'3/1/2020'
FROM [STG].[JSTG_MANL_JE_DETAIL]
GROUP BY BCH_NO, [ACCT_NO]

--DROP TEMP TABLES
IF object_id(N'tempdb..#OWNERTEMP') IS NOT NULL DROP TABLE #OWNERTEMP
IF object_id(N'tempdb..#LSE_ADJ_AMT') IS NOT NULL DROP TABLE #LSE_ADJ_AMT

END 

GO



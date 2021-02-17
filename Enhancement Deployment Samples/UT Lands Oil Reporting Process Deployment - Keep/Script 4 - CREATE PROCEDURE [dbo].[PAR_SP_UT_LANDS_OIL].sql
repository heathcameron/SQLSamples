USE [QAS_PRD_PAR_CUSTOM]
GO

/****** Object:  StoredProcedure [dbo].[PAR_SP_UT_LANDS_OIL]    Script Date: 9/14/2020 10:17:15 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO

--DROP PROCEDURE [dbo].[PAR_SP_UT_LANDS_OIL]
CREATE PROCEDURE [dbo].[PAR_SP_UT_LANDS_OIL]

(@IN_UPDT_USER VARCHAR(30)
,@IN_UPDT_DT DATETIME
,@IN_PROCESS_QUEUE_ID NUMERIC(10,0)
,@IN_PROCESS_ID VARCHAR(10)
,@IN_PROCESS_STEP_QUEUE_ID NUMERIC(10,0)
,@IN_PROCESS_STEP_ID VARCHAR(10)
,@IN_MASTER_PROCESS_QUEUE_ID NUMERIC(10,0)
,@PROD_DT_INPUT DATETIME
)

AS

DECLARE
  @IN_ERR_MSG   varchar(256)
 ,@IN_LOG_MSG_XTRA1 varchar(256)

BEGIN TRY

BEGIN TRAN


---------------------  


DECLARE @PROD_DATE DATETIME
DECLARE @RPT_DATE VARCHAR(6)
DECLARE @CHK_COUNT CHAR(6)

SET @PROD_DATE = @PROD_DT_INPUT
SET @RPT_DATE = CONCAT(MONTH(@PROD_DATE),YEAR(@PROD_DATE))
SET @IN_ERR_MSG = 'ERROR MESSAGE: This month and product has already been finalized and reported'
SET @IN_LOG_MSG_XTRA1 = 'Error in executing stored procedure: ' + (select  DB_NAME() +'.' + OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID))
SET @CHK_COUNT = (Select count(*) from PAR_UPS16_QRA.dbo.[RONL_MANL_CHK_HDR] WHERE  CONCAT('UT-',@RPT_DATE,'-OIL') = CHK_NO and MANL_CHK_STAT_CD = 'PRO')

IF @CHK_COUNT > 0  

begin 

		EXEC PAR_UPS16_QFC..QPISA_PROCESS_MSG_LOG_ERR   
					 @IN_MASTER_PROCESS_QUEUE_ID
					,@IN_PROCESS_QUEUE_ID
					,@IN_PROCESS_ID
					,@IN_PROCESS_STEP_QUEUE_ID
					,@IN_PROCESS_STEP_ID
					,@IN_UPDT_USER
					,'CUSTOM'
					,@IN_ERR_MSG 
					,@IN_LOG_MSG_XTRA1

end 

else
--IF @CHK_COUNT = 0  

begin 

DELETE FROM QAS_PRD_PAR_CUSTOM.[dbo].[UT_LANDS_REPORT]
WHERE [PROD Date] = @RPT_DATE

	IF object_id(N'tempdb..#RD_GRS_VALUE') IS NOT NULL
		DROP TABLE #RD_GRS_VALUE

	IF object_id(N'tempdb..#MAX_PRICE') IS NOT NULL
		DROP TABLE #MAX_PRICE

SELECT SUM(ACT_OWNR_GRS_AMT) + SUM(SOD_OWNR_REDSTRB_AMT) AS GRS_VALUE,
SUM(ACT_OWNR_GRS_VOL) AS GROSS_VOL, 
R.PROP_NO, 
SUM(CASE WHEN OWNR_INT_TYPE_CD = 'UT' THEN ACT_OWNR_TRANS_AMT ELSE 0 END) AS UT_AMOUNT,
CASE WHEN SUM(CASE WHEN OWNR_INT_TYPE_CD = 'UT' THEN ACT_OWNR_GRS_VOL ELSE 0 END) = 0 THEN 0 ELSE SUM(CASE WHEN OWNR_INT_TYPE_CD = 'UT' THEN ACT_OWNR_TRANS_AMT ELSE 0 END)/SUM(CASE WHEN OWNR_INT_TYPE_CD = 'UT' THEN ACT_OWNR_GRS_VOL ELSE 0 END) END UT_PRICE,
SUM(CASE WHEN OWNR_INT_TYPE_CD = 'UT' THEN ACT_OWNR_GRS_VOL ELSE 0 END) AS UT_VOL
, PROD_CD
INTO #RD_GRS_VALUE 
FROM PAR_UPS16_QRA.dbo.RTRN_RD_OWNR_LVL_PSUM_HIST R
INNER JOIN (SELECT DISTINCT PROP_NO FROM PAR_UPS16_QRA.dbo.DONL_DO_DETAIL WHERE INT_TYPE_CD = 'UT' AND EFF_DT_TO = '12/31/9999') M
ON R.PROP_NO = M.PROP_NO 
WHERE R.MAJ_PROD_CD = '100' 
AND PRDN_DT = @PROD_DATE
--AND OWNR_BA_NO = '6439'
--AND RVNU_RUN_ID <> '10061'
GROUP BY R.PROP_NO, PROD_CD

SELECT MAX(UT_PRICE) AS UT_PRICE, 
UNIT_NO,
LSE_NO
INTO #MAX_PRICE
FROM #RD_GRS_VALUE R
LEFT JOIN (SELECT DISTINCT MIN(UNIV_LSE_NO) UNIV_LSE_NO, PROP_NO, UNIT_NO, SUM(TRACT_DEC * RYL_DEC) AS BLENDED_RATE, SUM(TRACT_DEC) AS TRACT_DEC FROM PAR_UPS16_QRA.dbo.TONL_TX_TRRC_LSE_UT_MST  WHERE MAJ_PROD_CD = '100' GROUP BY PROP_NO, UNIT_NO) I
ON I.PROP_NO = R.PROP_NO
LEFT JOIN PAR_UPS16_QRA.dbo.TONL_TX_CPA_MST M
ON R.PROP_NO = M.WELL_NO
group by LSE_NO
,UNIT_NO
--SELECT * from #RD_GRS_VALUE
--where PROP_NO IN ('10194J')
--'10074J')

--SELECT * FROM QAS_PRD_PAR_CUSTOM.[dbo].[UT_LANDS_REPORT]
INSERT INTO QAS_PRD_PAR_CUSTOM.[dbo].[UT_LANDS_REPORT]
           ([Report Type]
           ,[Line Type]
           ,[Product]
           ,[PROD Date]
           ,[RRC Type]
           ,[RRC District]
           ,[RRC / Permit #]
           ,[Unit #]
           ,[Univ Lse #]
           ,[Beg Inv#]
           ,[8/8 Production]
           ,[8/8 Disposition]
           ,[End Inv]
           ,[Your Volume]
           ,[8/8 Notes]
           ,[API Gravity]
           ,[Oil Type]
           ,[BTU Factor]
           ,[Mkt Val]
           ,[Roy Due]
           ,[Tract LSE]
           ,[Tract RRC Number]
           ,[Trace No#]
           ,[MKT Val1]
           ,[Roy Due1]
           ,[Disp Code]
           ,[Volume]
           ,[MKT Val2]
           ,[ROY Due2]
           ,[Affiliated?]
           ,[Purchaser]
           ,[Payor])
SELECT   'O'
           ,'PROP'
           ,'OIL'
           ,@RPT_DATE
           ,'O'
           ,'8'
           ,CASE WHEN M.LSE_NO LIKE '0%' THEN RIGHT(M.LSE_NO,5) ELSE M.LSE_NO END
           ,I.UNIT_NO
           ,I.UNIV_LSE_NO
           ,COALESCE(SUM(BEG_INV),0)
           ,COALESCE(SUM(PROD),0)
           ,COALESCE((SUM(SALES)+SUM(SKIM)),0)
           ,COALESCE(SUM([END_INV]),0)
           ,COALESCE((SUM(SALES)+SUM(SKIM)),0)
           ,NULL
           ,MIN(GRAV)
           ,'1'
           ,NULL
           ,COALESCE((SUM(SALES)+SUM(SKIM))* MAX(UT_PRICE) ,0)
           ,COALESCE((SUM(SALES)+SUM(SKIM))* MAX(UT_PRICE)* (SUM(BLENDED_RATE)/COUNT(DISTINCT I.PROP_NO)),0)
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
from QAS_PRD_PAR_CUSTOM.dbo.UT_OIL_PROD H
LEFT JOIN PAR_UPS16_QRA.dbo.TONL_TX_CPA_MST M
ON M.WELL_NO = H.WELL_NO
--LEFT JOIN TONL_TX_TRRC_LSE_UT_MST T
--ON T.PROP_NO = H.WELL_NO 
--AND M.PROP_NO = H.PROP_NO 
LEFT JOIN (SELECT DISTINCT MIN(UNIV_LSE_NO) UNIV_LSE_NO, PROP_NO, UNIT_NO, SUM(TRACT_DEC * RYL_DEC) AS BLENDED_RATE, SUM(TRACT_DEC) AS TRACT_DEC FROM PAR_UPS16_QRA.dbo.TONL_TX_TRRC_LSE_UT_MST  WHERE MAJ_PROD_CD = '100' GROUP BY PROP_NO, UNIT_NO) I
ON I.PROP_NO = H.WELL_NO
 LEFT JOIN #MAX_PRICE R
ON M.LSE_NO = R.LSE_NO
AND (I.UNIT_NO = R.UNIT_NO 
OR I.UNIT_NO IS NULL)
--AND T.PROP_NO = R.PROP_NO
WHERE M.EFF_DT_TO = '12/31/9999'
GROUP BY 
M.LSE_NO
,I.UNIT_NO
,I.UNIV_LSE_NO

UNION 

SELECT   'O'
           ,'DISP'
           ,'OIL'
           ,@RPT_DATE
           ,'O'
           ,'8'
           ,CASE WHEN M.LSE_NO LIKE '0%' THEN RIGHT(M.LSE_NO,5) ELSE M.LSE_NO END
           ,I.UNIT_NO
           ,I.UNIV_LSE_NO
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,'0'
           ,COALESCE(SUM(SALES), 0) 
           ,COALESCE(SUM(SALES)* MAX(UT_PRICE),0)
           ,COALESCE(SUM(SALES)* MAX(UT_PRICE) * SUM(BLEND) ,0)/count(I.PROP_NO)
           ,NULL
           ,NULL
           ,NULL
from (SELECT DISTINCT  MIN(UNIV_LSE_NO) AS UNIV_LSE_NO, PROP_NO, UNIT_NO, SUM(TRACT_DEC * RYL_DEC) AS BLEND  FROM PAR_UPS16_QRA.dbo.TONL_TX_TRRC_LSE_UT_MST WHERE MAJ_PROD_CD = '100' GROUP BY PROP_NO, UNIT_NO) I 
LEFT JOIN PAR_UPS16_QRA.dbo.TONL_TX_CPA_MST M
ON M.WELL_NO = I.PROP_NO
--LEFT JOIN TONL_TX_TRRC_LSE_UT_MST T
--ON T.PROP_NO = H.WELL_NO 
--AND M.WELL_NO = H.WELL_NO 
INNER JOIN QAS_PRD_PAR_CUSTOM.dbo.UT_OIL_PROD H
ON I.PROP_NO = H.WELL_NO
 LEFT JOIN #MAX_PRICE R
ON M.LSE_NO = R.LSE_NO
AND (I.UNIT_NO = R.UNIT_NO 
OR I.UNIT_NO IS NULL)
--AND T.PROP_NO = R.PROP_NO
WHERE M.EFF_DT_TO = '12/31/9999'
GROUP BY 
M.LSE_NO
,I.UNIT_NO
,I.UNIV_LSE_NO


UNION 

SELECT   'O'
           ,'DISP'
           ,'OIL'
           ,@RPT_DATE
           ,'O'
           ,'8'
           ,CASE WHEN M.LSE_NO LIKE '0%' THEN RIGHT(M.LSE_NO,5) ELSE M.LSE_NO END
           ,I.UNIT_NO
           ,I.UNIV_LSE_NO
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,'8'
           ,COALESCE(SUM(SKIM), 0) 
           ,COALESCE(SUM(SKIM)* MAX(UT_PRICE),0)
           ,COALESCE(SUM(SKIM)* MAX(UT_PRICE) * SUM(BLEND) ,0)/count(distinct I.PROP_NO)
           ,NULL
           ,NULL
           ,NULL
from (SELECT DISTINCT  MIN(UNIV_LSE_NO) AS UNIV_LSE_NO, PROP_NO, UNIT_NO, SUM(TRACT_DEC * RYL_DEC) AS BLEND  FROM PAR_UPS16_QRA.dbo.TONL_TX_TRRC_LSE_UT_MST WHERE MAJ_PROD_CD = '100' GROUP BY PROP_NO, UNIT_NO) I 
LEFT JOIN PAR_UPS16_QRA.dbo.TONL_TX_CPA_MST M
ON M.WELL_NO = I.PROP_NO
--LEFT JOIN TONL_TX_TRRC_LSE_UT_MST T
--ON T.PROP_NO = H.WELL_NO 
--AND M.WELL_NO = H.WELL_NO 
INNER JOIN QAS_PRD_PAR_CUSTOM.dbo.UT_OIL_PROD H
ON I.PROP_NO = H.WELL_NO
 LEFT JOIN #MAX_PRICE R
ON M.LSE_NO = R.LSE_NO
AND (I.UNIT_NO = R.UNIT_NO 
OR I.UNIT_NO IS NULL)
--AND T.PROP_NO = R.PROP_NO
WHERE M.EFF_DT_TO = '12/31/9999'
--AND R.PROD_CD = '100'

GROUP BY 
M.LSE_NO
,I.UNIT_NO
,I.UNIV_LSE_NO


UNION 

SELECT DISTINCT  'O'
           ,'TRACT'
           ,'OIL'
           ,@RPT_DATE
           ,'O'
           ,'8'
           ,CASE WHEN M.LSE_NO LIKE '0%' THEN RIGHT(M.LSE_NO,5) ELSE M.LSE_NO END
           ,I.UNIT_NO
           ,I.UNIV_LSE_NO
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,T.UNIV_LSE_NO 
           ,CASE WHEN M.LSE_NO LIKE '0%' THEN RIGHT(M.LSE_NO,5) ELSE M.LSE_NO END
           ,PROP_ALIAS
           ,COALESCE(SUM(SALES)* MAX(UT_PRICE) * TRACT_DEC,0)
           ,COALESCE(SUM(SALES)* MAX(UT_PRICE) * TRACT_DEC * RYL_DEC,0)
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
from QAS_PRD_PAR_CUSTOM.dbo.UT_OIL_PROD H
LEFT JOIN PAR_UPS16_QRA.dbo.TONL_TX_CPA_MST M
ON M.WELL_NO = H.WELL_NO
LEFT JOIN PAR_UPS16_QRA.dbo.TONL_TX_TRRC_LSE_UT_MST T
ON T.PROP_NO = H.WELL_NO 
and T.MAJ_PROD_CD = '100' 
LEFT JOIN (SELECT DISTINCT MIN(UNIV_LSE_NO) UNIV_LSE_NO, PROP_NO, UNIT_NO FROM PAR_UPS16_QRA.dbo.TONL_TX_TRRC_LSE_UT_MST  WHERE MAJ_PROD_CD = '100' GROUP BY PROP_NO, UNIT_NO) I
ON I.PROP_NO = H.WELL_NO
 LEFT JOIN #MAX_PRICE R
ON M.LSE_NO = R.LSE_NO
AND (I.UNIT_NO = R.UNIT_NO 
OR I.UNIT_NO IS NULL)
--AND T.PROP_NO = R.PROP_NO
WHERE M.EFF_DT_TO = '12/31/9999'
AND I.UNIT_NO IS NOT NULL 
GROUP BY 
M.LSE_NO
,I.UNIT_NO
,I.UNIV_LSE_NO
,PROP_ALIAS
,TRACT_DEC
,RYL_DEC
,T.UNIV_LSE_NO

UNION

SELECT   'O'
           ,'PURCH'
           ,'OIL'
           ,@RPT_DATE
           ,'O'
           ,'8'
           ,CASE WHEN LSE_NO LIKE '0%' THEN RIGHT(LSE_NO,5) ELSE LSE_NO END
           ,UNIT_NO
           ,I.UNIV_LSE_NO
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,NULL
           ,'N'
           ,PURCH
           ,'642652'
from QAS_PRD_PAR_CUSTOM.dbo.UT_OIL_PROD H
LEFT JOIN PAR_UPS16_QRA.dbo.TONL_TX_CPA_MST M
ON M.WELL_NO = H.WELL_NO
--LEFT JOIN TONL_TX_TRRC_LSE_UT_MST T
--ON T.PROP_NO = H.WELL_NO 
--AND M.PROP_NO = H.PROP_NO 
LEFT JOIN (SELECT DISTINCT MIN(UNIV_LSE_NO) UNIV_LSE_NO, PROP_NO, UNIT_NO FROM PAR_UPS16_QRA.dbo.TONL_TX_TRRC_LSE_UT_MST WHERE MAJ_PROD_CD = '100' GROUP BY PROP_NO, UNIT_NO) I
ON I.PROP_NO = H.WELL_NO
 LEFT JOIN #RD_GRS_VALUE R
ON M.WELL_NO = R.PROP_NO
AND H.WELL_NO = R.PROP_NO 
--AND T.PROP_NO = R.PROP_NO
WHERE M.EFF_DT_TO = '12/31/9999'
GROUP BY 
LSE_NO
,UNIT_NO
,I.UNIV_LSE_NO
, PURCH

	IF object_id(N'tempdb..#GROUPED_RPT') IS NOT NULL
		DROP TABLE #GROUPED_RPT

	IF object_id(N'tempdb..#RPT_AT_PROP') IS NOT NULL
		DROP TABLE #RPT_AT_PROP

	IF object_id(N'tempdb..#PROP_LVL_SALES') IS NOT NULL
		DROP TABLE #PROP_LVL_SALES

DELETE FROM QAS_PRD_PAR_CUSTOM.[dbo].[UT_LANDS_VERIFY_RPT] 

SELECT SUM(Volume) AS Volume, 
SUM([MKT Val2]) AS [MKT Val2], 
Sum([ROY Due2]) AS [ROY Due2] , 
[RRC / Permit #], 
[Unit #] 
INTO #GROUPED_RPT 
FROM QAS_PRD_PAR_CUSTOM.[dbo].[UT_LANDS_REPORT]
WHERE [Line Type] = 'DISP'
AND [PROD Date] = @RPT_DATE
GROUP BY  [RRC / Permit #],
           [Unit #]

		   
SELECT R.WELL_NO,
           LSE_NO,
           UNIT_NO,
		   SUM(SALES) + SUM(SKIM) AS PROP_NO_SALES
		   INTO #PROP_LVL_SALES
		   FROM QAS_PRD_PAR_CUSTOM.[dbo].UT_OIL_PROD R
LEFT JOIN PAR_UPS16_QRA.dbo.TONL_TX_CPA_MST M
ON M.WELL_NO = R.WELL_NO
AND EFF_DT_TO = '12/31/9999'
--AND UNIT_NO = [Unit #]
LEFT JOIN (SELECT DISTINCT PROP_NO, UNIT_NO FROM PAR_UPS16_QRA.dbo.TONL_TX_TRRC_LSE_UT_MST WHERE MAJ_PROD_CD = '100') I
ON I.PROP_NO = R.WELL_NO
GROUP BY R.WELL_NO,
           LSE_NO,
           UNIT_NO


INSERT INTO QAS_PRD_PAR_CUSTOM.[dbo].[UT_LANDS_VERIFY_RPT]
           ([PROP_NO]
           ,[LSE_NO]
           ,[UNIT_NO]
           ,[RD_AMT_PROP_GRS_VAL]
           ,[RPT_AMT_PROP_GRS_VAL]
           ,[RD_AMT_PROP_GRS_VOL]
           ,[RPT_AMT_PROP_GRS_VOL]
           ,[RD_PRICE]
           ,[RPT_PRICE]
           ,[RD_UT_ROY_DUE]
           ,[RPT_UT_ROY_DUE]
           ,[UT DIFF])
SELECT 
           U.WELL_NO,
           LSE_NO,
           UNIT_NO,
           COALESCE(SUM(GRS_VALUE),0),
           NULL, --RPT_AMT_PROP_GRS_VAL,
           COALESCE(SUM(GROSS_VOL),0),
           NULL, --RPT_AMT_PROP_GRS_VOL,
           COALESCE(MAX(UT_PRICE),0),
           NULL, --RPT_PRICE, 
           COALESCE(SUM(UT_AMOUNT),0),
           NULL, --RPT_UT_ROY_DUE,
           NULL --UT DIFF
FROM QAS_PRD_PAR_CUSTOM.[dbo].UT_OIL_PROD U
LEFT JOIN #RD_GRS_VALUE V
ON V.PROP_NO = U.WELL_NO
LEFT JOIN (SELECT DISTINCT PROP_NO, UNIT_NO FROM PAR_UPS16_QRA.dbo.TONL_TX_TRRC_LSE_UT_MST WHERE MAJ_PROD_CD = '100') I
on U.WELL_NO = I.PROP_NO
LEFT JOIN PAR_UPS16_QRA.dbo.TONL_TX_CPA_MST M
ON M.WELL_NO = U.WELL_NO
AND EFF_DT_TO = '12/31/9999'
--WHERE LSE_NO = '047721'
GROUP BY U.WELL_NO,
           LSE_NO,
           UNIT_NO
--SELECT * fROM #RD_GRS_VALUE

--SELECT * fROM #RD_GRS_VALUE


SELECT M.WELL_NO,
           M.LSE_NO,
           [Unit #] AS UNIT_NO,
		   SUM(Volume) *  CASE WHEN SUM(Volume) = 0 THEN 0 ELSE (SUM(L.PROP_NO_SALES)/SUM(Volume)) END  AS RPT_AMT_PROP_GRS_VOL, 
		   SUM([MKT Val2]) *  CASE WHEN SUM(Volume) = 0  THEN 0 ELSE (SUM(L.PROP_NO_SALES)/SUM(Volume)) END AS RPT_AMT_PROP_GRS_VAL , 
		   Sum([ROY Due2])  *  CASE WHEN SUM(Volume) = 0  THEN 0 ELSE (SUM(L.PROP_NO_SALES)/SUM(Volume)) END AS RPT_UT_ROY_DUE 
		   into #RPT_AT_PROP
		   FROM #GROUPED_RPT R
LEFT JOIN PAR_UPS16_QRA.dbo.TONL_TX_CPA_MST M
ON [RRC / Permit #] = CASE WHEN LSE_NO LIKE '0%' THEN RIGHT(LSE_NO,5) ELSE LSE_NO END
AND EFF_DT_TO = '12/31/9999'
--AND UNIT_NO = [Unit #]
LEFT JOIN #PROP_LVL_SALES L
ON L.WELL_NO = M.WELL_NO
 AND  L.LSE_NO=  M.LSE_NO
 AND  (L.UNIT_NO = R.[Unit #]
 OR [Unit #] IS NULL)
 WHERE L.WELL_NO IS NOT NULL 
GROUP BY M.WELL_NO,
           M.LSE_NO,
           [Unit #]

UPDATE R
SET RPT_AMT_PROP_GRS_VAL = P.RPT_AMT_PROP_GRS_VAL
, RPT_AMT_PROP_GRS_VOL = P.RPT_AMT_PROP_GRS_VOL
, RPT_PRICE = CASE WHEN P.RPT_AMT_PROP_GRS_VOL = 0 THEN 0 ELSE P.RPT_AMT_PROP_GRS_VAL/ P.RPT_AMT_PROP_GRS_VOL END
, RPT_UT_ROY_DUE = P.RPT_UT_ROY_DUE
, [UT DIFF] =  P.RPT_UT_ROY_DUE - R.RD_UT_ROY_DUE
FROM QAS_PRD_PAR_CUSTOM.[dbo].[UT_LANDS_VERIFY_RPT] R
INNER JOIN #RPT_AT_PROP P
ON P.WELL_NO =  R.PROP_NO
 AND P.LSE_NO = R.LSE_NO
 AND (P.UNIT_NO = R.UNIT_NO
 OR R.UNIT_NO IS NULL)


 --SELECT * FROM QAS_PRD_PAR_CUSTOM.[dbo].[UT_LANDS_VERIFY_RPT]

   DELETE D FROM PAR_UPS16_QRA.dbo.[RONL_MANL_CHK_DETAIL] D
  INNER JOIN PAR_UPS16_QRA.dbo.[RONL_MANL_CHK_HDR] H
  ON H.[MANL_CHK_SEQ_NO] = D.[MANL_CHK_SEQ_NO]
  WHERE CHK_NO = CONCAT('UT-',@RPT_DATE,'-OIL')

 DELETE FROM PAR_UPS16_QRA.dbo.[RONL_MANL_CHK_HDR] WHERE CHK_NO = CONCAT('UT-',@RPT_DATE,'-OIL')

 INSERT INTO PAR_UPS16_QRA.dbo.[RONL_MANL_CHK_HDR]
           ([MANL_CHK_SEQ_NO]
           ,[OPER_BUS_SEG_CD]
           ,[MANL_CHK_STAT_CD]
           ,[BUS_UNIT_CD]
           ,[BANK_ACCT_NO]
           ,[CHK_NO]
           ,[CHK_DT]
           ,[CHK_AMT]
           ,[OWNR_NO]
           ,[OWNR_SUB]
           ,[CMNT_SEQ_NO]
           ,[PROCESS_QUEUE_ID]
           ,[PROCESS_STEP_QUEUE_ID]
           ,[BANK_NO]
           ,[APRV_USER]
           ,[APRV_DT]
           ,[CREATE_USER]
           ,[UPDT_USER]
           ,[UPDT_DT])
SELECT  (select LAST_NO + 1 from PAR_UPS16_QFC.dbo.QARCH_TRAN_SEQ WHERE DBCOL_NM = 'MANL_CHK_SEQ_NO')
           ,'PAR' --OPER_BUS_SEG_CD 
           ,'VAL' --MANL_CHK_STAT_CD 
           ,'001' --BUS_UNIT_CD
           ,'12345678'
           ,CONCAT('UT-',@RPT_DATE,'-OIL') --CHK_NO
           ,@PROD_DATE
           ,SUM(RPT_UT_ROY_DUE) --CHK_AMT
           ,'6439'
           ,'1'
           ,NULL
           ,NULL
           ,NULL
           ,'BANK'
           ,NULL
           ,NULL
           ,'UT_LAND_PROC'
           ,'UT_LAND_PROC'
           ,CONVERT(SMALLDATETIME, Getdate())
FROM #RPT_AT_PROP 


INSERT INTO PAR_UPS16_QRA.dbo.[RONL_MANL_CHK_DETAIL]
           ([MANL_CHK_SEQ_NO]
           ,[OPER_BUS_SEG_CD]
           ,[LINE_NO]
           ,[PROP_NO]
           ,[DO_TYPE_CD]
           ,[DO_MAJ_PROD_CD]
           ,[TIER]
           ,[OWNR_INT_TYPE_CD]
           ,[OWNR_INT_TYPE_SEQ_NO]
           ,[PRDN_DT]
           ,[PROD_CD]
           ,[DISP_CD]
           ,[CTR_NO]
           ,[OWNR_NRI_DEC]
           ,[OWNR_BAL_DEC]
           ,[GRS_QTY]
           ,[GRS_AMT]
           ,[TOT_SEV_TAX_REIMB_LSE_GRS_AMT]
           ,[TOT_SEV_TAX_LSE_GRS_AMT]
           ,[TOT_ADJ_CTGY_LSE_GRS_AMT]
           ,[TRANS_QTY]
           ,[TRANS_VAL_AMT]
           ,[TOT_SEV_TAX_REIMB_NET_AMT]
           ,[TOT_SEV_TAX_NET_AMT]
           ,[TOT_ADJ_CTGY_NET_AMT]
           ,[OTH_CKW_ADJ_CD]
           ,[OTH_CKW_ADJ_AMT]
           ,[TRANS_AMT]
           ,[BTU_FACT]
           ,[BTU_BASIS_CD]
           ,[STD_PRES]
           ,[STD_GRV]
           ,[VOL_CLASS_CD]
           ,[PAY_CD]
           ,[UNIT_PROP_NO]
           ,[UPDT_USER]
           ,[UPDT_DT]
           ,[WELL_NO]
           ,[COMPL_NO])
SELECT  (select LAST_NO + 1 from PAR_UPS16_QFC.dbo.QARCH_TRAN_SEQ WHERE DBCOL_NM = 'MANL_CHK_SEQ_NO')
           ,'PAR' --OPER_BUS_SEG_CD
           ,ROW_NUMBER() OVER(ORDER BY R.WELL_NO ASC)  --LINE_NO
           ,R.WELL_NO--,PROP_NO
           ,'REV' --DO_TYPE_CD
           ,'ALL'--DO_MAJ_PROD_CD
           ,COALESCE(X.TIER,1)
           ,'UT'--OWNR_INT_TYPE_CD
           ,'1'--OWNR_INT_TYPE_SEQ_NO
           ,@PROD_DATE
           ,'100'--PROD_CD
           ,'03' --DISP_CD
           ,''--CTR_NO
           ,COALESCE(NRI_DEC, 1) --OWNR_NRI_DEC
           ,COALESCE(NRI_DEC, 1) --OWNR_BAL_DEC
           ,RPT_AMT_PROP_GRS_VOL --GRS_QTY
           ,RPT_AMT_PROP_GRS_VAL 
           ,0--TOT_SEV_TAX_REIMB_LSE_GRS_AMT
           ,0--TOT_SEV_TAX_LSE_GRS_AMT
           ,0--TOT_ADJ_CTGY_LSE_GRS_AMT
           ,'0'--TRANS_QTY
           ,RPT_UT_ROY_DUE
           ,0--TOT_SEV_TAX_REIMB_NET_AMT
           ,0--TOT_SEV_TAX_NET_AMT
           ,0--TOT_ADJ_CTGY_NET_AMT
           ,NULL--OTH_CKW_ADJ_CD
           ,0--OTH_CKW_ADJ_AMT
           ,RPT_UT_ROY_DUE --TRANS_AMT
           ,NULL--BTU_FACT
           ,NULL--BTU_BASIS_CD
           ,NULL--STD_PRES
           ,'40'--STD_GRV
           ,'A'--VOL_CLASS_CD
           ,'1'--PAY_CD
           ,NULL--UNIT_PROP_NO
           ,'UT_LAND_PROC'
           ,CONVERT(SMALLDATETIME, Getdate())
           ,R.WELL_NO
           ,COMPL_NO
FROM #RPT_AT_PROP R
LEFT JOIN PAR_UPS16_QRA.dbo.PXRF_WELL_COMPL_PROP_DO_QRA X
ON R.WELL_NO = X.WELL_NO
AND EFF_DT_FROM = '12/31/9999'
AND MAJ_PROD_CD = '100'
AND COMPL_NO = 1
LEFT JOIN (SELECT SUM(NRI_DEC) AS NRI_DEC, TIER, PROP_NO FROM PAR_UPS16_QRA.dbo.DONL_DO_DETAIL WHERE EFF_DT_TO = '12/31/9999' AND INT_TYPE_CD = 'UT' AND DO_TYPE_CD = 'REV' GROUP BY TIER, PROP_NO) D
ON X.TIER = D.TIER
AND X.PROP_NO = D.PROP_NO
WHERE  RPT_UT_ROY_DUE <> 0 

--SELECT * FROM PAR_UPS16_QRA.dbo.DONL_DO_DETAIL
UPDATE PAR_UPS16_QFC.dbo.QARCH_TRAN_SEQ
SET LAST_NO = LAST_NO + 1 
WHERE DBCOL_NM = 'MANL_CHK_SEQ_NO'
 --SELECT * FROM QAS_PRD_PAR_CUSTOM.[dbo].[UT_LANDS_VERIFY_RPT]
--SELECT * FROM QAS_PRD_PAR_CUSTOM.[dbo].[UT_LANDS_REPORT]


	IF object_id(N'tempdb..#GROUPED_RPT') IS NOT NULL
		DROP TABLE #GROUPED_RPT

	IF object_id(N'tempdb..#RPT_AT_PROP') IS NOT NULL
		DROP TABLE #RPT_AT_PROP

	IF object_id(N'tempdb..#PROP_LVL_SALES') IS NOT NULL
		DROP TABLE #PROP_LVL_SALES

	IF object_id(N'tempdb..#RD_GRS_VALUE') IS NOT NULL
		DROP TABLE #RD_GRS_VALUE


end
---------------------  

COMMIT

END TRY

BEGIN CATCH

	ROLLBACK
	SET @IN_ERR_MSG  = ERROR_MESSAGE()
	SET @IN_LOG_MSG_XTRA1 = 'Error in executing stored procedure: ' + (select  DB_NAME() +'.' + OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID))

	EXEC PAR_UPS16_QFC..QPISA_PROCESS_MSG_LOG_ERR   
					 @IN_MASTER_PROCESS_QUEUE_ID
					,@IN_PROCESS_QUEUE_ID
					,@IN_PROCESS_ID
					,@IN_PROCESS_STEP_QUEUE_ID
					,@IN_PROCESS_STEP_ID
					,@IN_UPDT_USER
					,'CUSTOM'
					,@IN_ERR_MSG
					,@IN_LOG_MSG_XTRA1

END CATCH






GO



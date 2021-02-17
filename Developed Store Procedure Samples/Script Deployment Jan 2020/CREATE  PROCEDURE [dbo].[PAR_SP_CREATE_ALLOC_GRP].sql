USE [QAS_PRD_PAR_CUSTOM]
GO

/****** Object:  StoredProcedure [dbo].[PAR_SP_CREATE_ALLOC_GRP]    Script Date: 1/6/2020 6:03:46 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO



CREATE  PROCEDURE [dbo].[PAR_SP_CREATE_ALLOC_GRP]

(@IN_UPDT_USER VARCHAR(30)
,@IN_UPDT_DT DATETIME
,@IN_PROCESS_QUEUE_ID NUMERIC(10,0)
,@IN_PROCESS_ID VARCHAR(10)
,@IN_PROCESS_STEP_QUEUE_ID NUMERIC(10,0)
,@IN_PROCESS_STEP_ID VARCHAR(10)
,@IN_MASTER_PROCESS_QUEUE_ID NUMERIC(10,0)
)

AS

DECLARE
  @IN_ERR_MSG   varchar(256)
 ,@IN_LOG_MSG_XTRA1 varchar(256)

 BEGIN TRY

--Business logic below
---------------------  


--STEP 1: CAPTURE CURRENT JIB MONTH
DECLARE @CURRENT_JIB_MTH DATETIME
DECLARE @END_OF_CURRENT_JIB DATETIME
DECLARE @END_OF_PREVIOUS_JIB DATETIME


SET @CURRENT_JIB_MTH = (SELECT DISTINCT MAX(PROCESS_PERIOD) FROM PAR_UPS16_QCA.dbo.JBCDE_BUSINESS_SEGMENT WHERE OPER_BUS_SEG_CD = 'PAR')
SET @END_OF_CURRENT_JIB = EOMONTH(@CURRENT_JIB_MTH)
SET @END_OF_PREVIOUS_JIB = EOMONTH(DATEADD(month, -1, @CURRENT_JIB_MTH))

--STEP 2:GRAB ACTIVE PROPERTIES WITH FOREMAN/PUMPER ATTRIBUTES

SELECT A.PROP_NO, PROP_NM INTO #ALLOCATION_PROP
FROM PAR_UPS16_QCA.dbo.GONL_PROP_ATTR A
LEFT JOIN PAR_UPS16_QCA.dbo.GONL_PROP P
ON P.PROP_NO = A.PROP_NO
WHERE ATTR_TYPE_CD = 'ALT' 
AND ATTR_VALUE = 'PF'
AND PROP_STAT_CD = 'A'
AND PROP_NM NOT LIKE '%ANNUAL%'
AND A.PROP_NO <> '999999'
 
--STEP 3: END DATE EXISTING JIB_ALLOCATION

UPDATE AA
SET AA.EFF_DT_TO = @END_OF_PREVIOUS_JIB
FROM  PAR_UPS16_QCA.dbo.JBONL_ALLOC_GRP AA
inner join  PAR_UPS16_QCA.dbo.JBONL_ALLOC_GRP_DETAIL BB on AA.AG_ID = BB.AG_ID 
inner join #ALLOCATION_PROP D on AA.FROM_PROP_NO = D.PROP_NO 
where EFF_DT_TO = '12/31/9999'

--STEP 4: CREATE ALLOCATION GROUP HEADER

INSERT INTO PAR_UPS16_QCA.dbo.[JBONL_ALLOC_GRP]
           ([OPER_BUS_SEG_CD]
           ,[AG_CODE]
           ,[EFF_DT_FROM]
           ,[EFF_DT_TO]
           ,[AG_NAME]
           ,[AG_TYPE]
           ,[AG_DETAIL_TYPE]
           ,[AG_DETAIL_VALUE]
           ,[ALLOCATION_FAMILY_ID]
           ,[AG_SEQUENCE]
           ,[AG_RULE]
           ,[FROM_PROP_NO]
           ,[EXTENDED]
           ,[UPDT_USER]
           ,[UPDT_DT]
           ,[USE_TO_AFE_IND])
select 'PAR', --OPER_BUS_SEG_CD,
PROP_NO, --AG_CODE AG_CODE,
@CURRENT_JIB_MTH, --EFF_DT_FROM EFF_DT_FROM,
@END_OF_CURRENT_JIB, --EFF_DT_TO EFF_DT_TO,
PROP_NM, --AG_NAME AG_NAME,
'GEN',--AG_TYPE AG_TYPE,
'GEN', --AG_DETAIL_TYPE AG_DETAIL_TYPE,
NULL, --AG_DETAIL_VALUE,
NULL, --ALLOCATION_FAMILY_ID,
'000', --AG_SEQUENCE
'WCT', --AG_RULE,
PROP_NO,
NULL, --EXTENDED,
'ALLOCATION_SP', --UPDT_USER,
getdate() UPDT_DT,
'0' --USE_TO_AFE_IND 
from #ALLOCATION_PROP D
LEFT JOIN PAR_UPS16_QCA.dbo.JBONL_ALLOC_GRP AA 
ON AA.FROM_PROP_NO = D.PROP_NO 
AND AA.EFF_DT_FROM = @CURRENT_JIB_MTH
WHERE AA.FROM_PROP_NO IS NULL 

--STEP 5: GRAB SEQUENCE NUMBER FOR ALLCATION GROUP HEADER PUMPER/FOREMAN

SELECT [AG_ID], FROM_PROP_NO 
INTO #AG_PROP_XREF 
FROM PAR_UPS16_QCA.dbo.JBONL_ALLOC_GRP
INNER JOIN #ALLOCATION_PROP
ON FROM_PROP_NO = PROP_NO
WHERE EFF_DT_FROM =  @CURRENT_JIB_MTH

--STEP 6: GRAB SEQUENCE NUMBER FOR ALLCATION GROUP HEADER BASIN

SELECT AG_ID, CASE WHEN FROM_PROP_NO = '999997' THEN 'MIDLAND'
ELSE 'DELAWARE' END BASIN INTO #AG_BASIN_XREF 
FROM PAR_UPS16_QCA.dbo.JBONL_ALLOC_GRP 
WHERE FROM_PROP_NO IN ('999997','999998') 
AND EFF_DT_FROM =  @CURRENT_JIB_MTH

--STEP 7: DELETE EXISTING DETAIL FROM HEADER 

DELETE D
FROM PAR_UPS16_QCA.dbo.[JBONL_ALLOC_GRP_DETAIL] D
INNER JOIN #AG_PROP_XREF X
ON X.[AG_ID] = D.[AG_ID]

--STEP 8: CREATE PUMPER AND FOREMAN ALLOCATION GROUP DETAIL 

INSERT INTO PAR_UPS16_QCA.dbo.[JBONL_ALLOC_GRP_DETAIL]
           ([AG_ID]
           ,[TO_PROP_NO]
           ,[ALLOCATION_BASIS]
           ,[UPDT_USER]
           ,[UPDT_DT]
           ,[ACCOUNT_CODE]
           ,[TO_AFE_NO]
           ,[TO_TIER])
SELECT 
AG_ID,
A.PROP_NO, --TO_PROP_NO,
1,--ALLOCATION_BASIS,
'ALLOCATION_SP', --UPDT_USER,
getdate(), --UPDT_DT,
NULL,--ACCOUNT_CODE,
NULL,--TO_AFE_NO,
NULL--TO_TIER
FROM #AG_PROP_XREF X
LEFT JOIN PAR_UPS16_QCA.dbo.GONL_PROP P
ON X.FROM_PROP_NO = P.PROP_NO 
LEFT JOIN PAR_UPS16_QCA.dbo.GONL_PROP_ATTR A
ON UPPER(PROP_NM) LIKE '%' + UPPER(ATTR_VALUE) + '%'
AND ATTR_TYPE_CD IN ('FOR', 'PUM')
LEFT JOIN PAR_UPS16_QCA.dbo.PONL_WELL_COMPL_EFF_DT
ON WELL_NO = A.PROP_NO
AND COMPL_NO = '1'
AND EFF_DT_TO = '12/31/9999'
WHERE A.PROP_NO IS NOT NULL
AND PROP_STAT_CD = 'A'
AND WELL_STAT_CD NOT IN ('4003',--SOLD
'4001' )--P&A
AND AG_ID NOT IN (SELECT AG_ID FROM #AG_BASIN_XREF)

--STEP 8: CREATE BASIN ALLOCATION GROUP DETAIL 

INSERT INTO PAR_UPS16_QCA.dbo.[JBONL_ALLOC_GRP_DETAIL]
           ([AG_ID]
           ,[TO_PROP_NO]
           ,[ALLOCATION_BASIS]
           ,[UPDT_USER]
           ,[UPDT_DT]
           ,[ACCOUNT_CODE]
           ,[TO_AFE_NO]
           ,[TO_TIER]
		   )
SELECT DISTINCT 
AG_ID,
A.PROP_NO, --TO_PROP_NO,
1,--ALLOCATION_BASIS,
'ALLOCATION_SP', --UPDT_USER,
getdate(), --UPDT_DT,
NULL,--ACCOUNT_CODE,
NULL,--TO_AFE_NO,
NULL--TO_TIER
FROM  PAR_UPS16_QCA.dbo.GONL_PROP P
INNER JOIN PAR_UPS16_QCA.dbo.GONL_PROP_ATTR A
ON P.PROP_NO = A.PROP_NO 
AND ATTR_TYPE_CD = 'BSN'
INNER JOIN PAR_UPS16_QCA.dbo.PONL_WELL_COMPL_EFF_DT
ON WELL_NO = A.PROP_NO
AND COMPL_NO = '1'
AND EFF_DT_TO = '12/31/9999'
INNER JOIN #AG_BASIN_XREF T
ON ATTR_VALUE = T.BASIN
WHERE PROP_STAT_CD = 'A'
AND WELL_STAT_CD NOT IN ('4003',--SOLD
'4001') --P&A
AND OPER_FL = 'Y'
AND ATTR_VALUE IN  ('DELAWARE','MIDLAND')
AND PROP_TYPE_CD = 'WL'
and WELL_CLASS_CD <> 'W'


--DROP TEMP TABLES
DROP TABLE #ALLOCATION_PROP
DROP TABLE #AG_PROP_XREF 
DROP TABLE #AG_BASIN_XREF

---------------------  


END TRY

BEGIN CATCH

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



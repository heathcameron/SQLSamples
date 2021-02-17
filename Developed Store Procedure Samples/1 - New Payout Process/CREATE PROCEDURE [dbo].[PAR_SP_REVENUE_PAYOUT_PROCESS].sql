USE [QAS_PRD_PAR_CUSTOM]
GO

/****** Object:  StoredProcedure [dbo].[PAR_SP_REVENUE_PAYOUT_PROCESS]    Script Date: 9/30/2019 4:53:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER OFF
GO


CREATE PROCEDURE [dbo].[PAR_SP_REVENUE_PAYOUT_PROCESS]

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

DELETE FROM [PAR_UPS16_QRA].[dbo].RONL_PO_DETAIL_RVNU WHERE RETAIN_SALES_OVRD_FL <> 'Y'

INSERT INTO [PAR_UPS16_QRA].[dbo].[RONL_PO_DETAIL_RVNU] 
            ([PROP_NO], 
             [OPER_BUS_SEG_CD], 
             [RVSL_FL], 
             [MAJ_PROD_CD], 
             [SALES_VOL], 
             [SALES_VOL_OVRD], 
             [SALES_PRC], 
             [SALES_PRC_OVRD], 
             [TAX_VAL], 
             [TAX_VAL_OVRD], 
             [RYLTY_RATE], 
             [RYLTY_RATE_OVRD], 
             [TAX_FREE_RATE], 
             [TAX_FREE_RATE_OVRD], 
             [RETAIN_SALES_OVRD_FL], 
             [CMNT_SEQ_NO], 
             [UPDT_USER], 
             [UPDT_DT], 
             [PRDN_DT], 
             [ACCTG_MTH]
			 ) 
SELECT VAL.[PROP_NO], 
       [OPER_BUS_SEG_CD], 
       'N', --[RVSL_FL] 
       [MAJ_PROD_CD], 
       [SALES_VOL], 
       NULL, --[SALES_VOL_OVRD] 
       [SALES_PRC], 
       NULL, --[SALES_PRC_OVRD] 
       [TAX_VAL], 
       NULL, --[TAX_VAL_OVRD] 
       [RYLTY_RATE], 
       NULL, --[RYLTY_RATE_OVRD] 
       [TAX_FREE_RATE], 
       NULL, --[TAX_FREE_RATE_OVRD] 
	   'N', --[RETAIN_SALES_OVRD_FL] 
       NULL, --[CMNT_SEQ_NO] 
       'PAR_SP_PAYOUTS',--[UPDT_USER], 
       CONVERT(SMALLDATETIME, Getdate()), --[UPDT_DT]  
       [PRDN_DT],
       ACCTG_MTH
FROM   (SELECT H.PROP_NO, 
               H.MAJ_PROD_CD, 
               Sum(COALESCE(ACT_OWNR_GRS_VOL, 0)) AS [SALES_VOL], 
               CASE 
                 WHEN Sum(COALESCE(ACT_OWNR_GRS_VOL, 0)) = 0 THEN 0 
                 ELSE ( Sum(COALESCE(ACT_OWNR_GRS_AMT, 0)) - 
                        Sum(COALESCE(ADJ_AMT, 0)) 
                      ) / Sum( 
                      ACT_OWNR_GRS_VOL) 
               END                                AS [SALES_PRC], 
               Sum(COALESCE(TAX_AMT, 0))          AS [TAX_VAL], 
               H.PRDN_DT, 
               MAX(H.ACCTG_MTH) AS ACCTG_MTH, 
               H.[OPER_BUS_SEG_CD] 
        FROM   [PAR_UPS16_QRA].[dbo].RTRN_RD_OWNR_LVL_PSUM_HIST H 
               LEFT JOIN (SELECT RVNU_RUN_ID, 
                                 TRANS_NO, 
                                 Sum(COALESCE(ACT_OWNR_ADJ_AMT,0))+ sum(COALESCE(REDSTRB_EXMPT_ADJ_AMT,0)) + sum(COALESCE(REDSTRB_SOD_ADJ_AMT,0)) AS ADJ_AMT 
                          FROM   [PAR_UPS16_QRA].[dbo].RTRN_RD_OWNR_LVL_ADJ_PSUM_HIST 
                          GROUP  BY RVNU_RUN_ID, 
                                    TRANS_NO) ADJ 
                      ON H.RVNU_RUN_ID = ADJ.RVNU_RUN_ID 
                         AND H.TRANS_NO = ADJ.TRANS_NO 
               LEFT JOIN (SELECT RVNU_RUN_ID, 
                                 TRANS_NO, 
                                 Sum(COALESCE(ACT_OWNR_TAX_AMT,0)) AS TAX_AMT 
                          FROM   [PAR_UPS16_QRA].[dbo].RTRN_RD_OWNR_LVL_TAX_PSUM_HIST 
                          GROUP  BY RVNU_RUN_ID, 
                                    TRANS_NO) TAX 
                      ON H.RVNU_RUN_ID = TAX.RVNU_RUN_ID 
                         AND H.TRANS_NO = TAX.TRANS_NO 
               INNER JOIN (SELECT DISTINCT PROP_NO FROM [PAR_UPS16_QRA].[dbo].RXRF_PO_PROP) X 
                       ON X.PROP_NO = H.PROP_NO 
               LEFT JOIN [PAR_UPS16_QRA].[dbo].RONL_PO_DETAIL_RVNU P 
                      ON P.PROP_NO = H.PROP_NO 
                         AND P.PRDN_DT = H.PRDN_DT 
                         AND P.MAJ_PROD_CD = H.MAJ_PROD_CD 
                         --AND P.ACCTG_MTH = H.ACCTG_MTH 
                         AND P.OPER_BUS_SEG_CD = H.OPER_BUS_SEG_CD 
        WHERE  ( RETAIN_SALES_OVRD_FL = 'N' 
                  OR RETAIN_SALES_OVRD_FL IS NULL ) 

        GROUP  BY H.PROP_NO, 
                  H.PRDN_DT, 
                  H.MAJ_PROD_CD, 
                  --H.ACCTG_MTH, 
                  H.[OPER_BUS_SEG_CD]) VAL 
       LEFT JOIN (SELECT D.PROP_NO, 
                         D.DO_TYPE_CD, 
                         D.DO_MAJ_PROD_CD, 
                         D.TIER, 
                         Sum(CASE 
                               WHEN INT_CTGY = 'NW' THEN NRI_DEC 
                               ELSE 0 
                             END)     AS [RYLTY_RATE], 
                         Sum(CASE 
                               WHEN ENTY_TYPE_CD IN ( '08', '04', '02' ) THEN 
                               NRI_DEC 
                               ELSE 0 
                             END)     AS [TAX_FREE_RATE], 
                         Sum(NRI_DEC) AS [DOI INTEREST] 
                  FROM   [PAR_UPS16_QRA].[dbo].DONL_DO_DETAIL D 
                         LEFT JOIN (SELECT DISTINCT PROP_NO, 
                                                    TIER, 
                                                    DO_TYPE_CD, 
                                                    DO_MAJ_PROD_CD, 
                                                    COMPL_NO 
                                    FROM   [PAR_UPS16_QRA].[dbo].PXRF_WELL_COMPL_PROP_DO_QRA 
                                    WHERE  COMPL_NO = '1' 
                                           AND EFF_DT_TO = '12/31/9999') X 
                                ON X.PROP_NO = D.PROP_NO 
                                   AND X.TIER = D.TIER 
                                   AND X.DO_TYPE_CD = D.DO_TYPE_CD 
                                   AND X.DO_MAJ_PROD_CD = D.DO_MAJ_PROD_CD 
                         LEFT JOIN [PAR_UPS16_QRA].[dbo].SCTRL_BA_ADDRESS BA_ADDRESS 
                                ON BA_ADDRESS.BA_NO = D.BA_NO 
                                   AND BA_ADDRESS.BA_SUF = D.BA_SUB 
                         LEFT JOIN [PAR_UPS16_QRA].[dbo].GCDE_INT_TYPE I 
                                ON D.INT_TYPE_CD = I.INT_TYPE_CD 
                  WHERE  D.EFF_DT_TO = '12/31/9999' 
                         AND X.COMPL_NO = '1' 
                         AND D.DO_TYPE_CD = 'REV' 
                  GROUP  BY D.PROP_NO, 
                            D.TIER, 
                            D.DO_TYPE_CD, 
                            D.DO_MAJ_PROD_CD) DOI 
              ON DOI.PROP_NO = VAL.PROP_NO 
			  WHERE ([SALES_VOL] <> 0 AND [TAX_VAL] <> 0)

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



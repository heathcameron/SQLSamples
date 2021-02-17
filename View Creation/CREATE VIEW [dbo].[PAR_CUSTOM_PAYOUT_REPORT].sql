USE [QAS_PRD_PAR_CUSTOM]
GO

/****** Object:  View [dbo].[PAR_CUSTOM_PAYOUT_REPORT]    Script Date: 1/4/2021 12:49:29 PM ******/
DROP VIEW [dbo].[PAR_CUSTOM_PAYOUT_REPORT]
GO

/****** Object:  View [dbo].[PAR_CUSTOM_PAYOUT_REPORT]    Script Date: 1/4/2021 12:49:29 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--drop view PAR_CUSTOM_PAYOUT_REPORT
CREATE VIEW [dbo].[PAR_CUSTOM_PAYOUT_REPORT] 
AS 
  SELECT PO.PAYOUT_ID, 
         PO.PROP_NO, 
         PROP_NM, 
         Max(HDR.DWE_PNLTY_PCT) 
            [PERCENT PAYOUT], 
         Max(HDR.UNL_MIN_INT_FL) 
         AS 
            [UNLEASED MINERALS], 
         PO.PRDN_DT 
         AS 
            [LAST PRODUCTION DATE], 
         Max(PO.ACCTG_MTH) 
         AS 
            [LAST ACCOUNTING DATE], 
         Sum(PO.PROP_BEG_BAL_VAL) 
         AS 
            [BEGINING VALUE], 
         Sum(PO.CALC_SALES_VAL) 
            [REVENUE GROSS], 
         Sum(PO.CALC_TAX_VAL) 
            [TAX], 
         Sum(PO.CALC_RYLTY_VAL) 
            [ROYALTY], 
         Sum(PO.CALC_SALES_VAL) - Sum(PO.CALC_TAX_VAL) - Sum(PO.CALC_RYLTY_VAL) 
         AS 
         [NET REVENUES], 
         Sum(PO.CALC_IDC_PNLTY_VAL) 
            [INTANGLBLES], 
         Sum(PO.CALC_DWE_PNLTY_VAL) 
            [DOWNHOLE/WELL], 
         Sum(PO.CALC_SUR_PNLTY_VAL) 
            [SURFACE], 
         Sum(PO.CALC_LOE_PNLTY_VAL) 
         AS 
            [OPERATING COSTS], 
         Sum(PO.CALC_IDC_PNLTY_VAL) 
         + Sum(PO.CALC_DWE_PNLTY_VAL) 
         + Sum(PO.CALC_SUR_PNLTY_VAL) 
         + Sum(PO.CALC_LOE_PNLTY_VAL) 
         AS 
            [TOTAL EXPENSE], 
         -( Sum(PO.PROP_BEG_BAL_VAL) - ( 
            Sum(PO.CALC_SALES_VAL) - Sum(PO.CALC_TAX_VAL) 
            - Sum 
               (PO.CALC_RYLTY_VAL) ) + 
               ( Sum(PO.CALC_IDC_PNLTY_VAL) 
                 + Sum(PO.CALC_DWE_PNLTY_VAL) 
                 + Sum(PO.CALC_SUR_PNLTY_VAL) 
                 + Sum(PO.CALC_LOE_PNLTY_VAL) ) ) 
            [NET CASH FLOW], 
         Max(PO2.RUNNING_TOTAL) 
            [PAYOUT AMOUNT TO RECOVER] 
  FROM   PAR_UPS16_QRA.dbo.RRPT_PO_PROP PO 
         LEFT JOIN PAR_UPS16_QRA.dbo.GONL_PROP P 
                ON PO.PROP_NO = P.PROP_NO 
         LEFT JOIN (SELECT DISTINCT PAYOUT_ID, 
                                    OPER_BUS_SEG_CD, 
                                    PAYOUT_TYPE_CD, 
                                    BUS_UNIT_CD, 
                                    PROP_NO, 
                                    PRDN_DT, 
                                    ACCTG_MTH, 
                                    RVSL_FL, 
                                    MAJ_PROD_CD, 
                                    -( Sum(PROP_BEG_BAL_VAL - ( 
                                           CALC_SALES_VAL - CALC_TAX_VAL 
                                           - CALC_RYLTY_VAL ) + ( 
                                              CALC_IDC_PNLTY_VAL + 
                                              CALC_DWE_PNLTY_VAL 
                                              + 
                                                  CALC_SUR_PNLTY_VAL + 
                                              CALC_LOE_PNLTY_VAL 
                                                                )) 
                                         OVER ( 
                                           PARTITION BY PAYOUT_ID, 
                                         OPER_BUS_SEG_CD 
                                         , 
                                         PAYOUT_TYPE_CD, 
                                         BUS_UNIT_CD, 
                                         PROP_NO 
                                           ORDER BY PRDN_DT) ) AS RUNNING_TOTAL 
                    FROM   PAR_UPS16_QRA.dbo.RRPT_PO_PROP) PO2 
                ON PO.PAYOUT_ID = PO2.PAYOUT_ID 
                   AND PO.OPER_BUS_SEG_CD = PO2.OPER_BUS_SEG_CD 
                   AND PO.PAYOUT_TYPE_CD = PO2.PAYOUT_TYPE_CD 
                   AND PO.BUS_UNIT_CD = PO2.BUS_UNIT_CD 
                   AND PO.PROP_NO = PO2.PROP_NO 
                   AND PO.PRDN_DT = PO2.PRDN_DT 
                   AND PO.ACCTG_MTH = PO2.ACCTG_MTH 
                   AND PO.RVSL_FL = PO2.RVSL_FL 
                   AND PO.MAJ_PROD_CD = PO2.MAJ_PROD_CD 
         LEFT JOIN PAR_UPS16_QRA.dbo.RONL_PO_HDR HDR 
                ON HDR.PAYOUT_ID = PO.PAYOUT_ID 
  GROUP  BY PO.PAYOUT_ID, 
            PO.PROP_NO, 
            PROP_NM, 
            PO.PRDN_DT 

GO



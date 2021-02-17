/*
*****READ ME*****
DESCRIPTION: REVENUE INTERNAL IMPACT OF REVERSE/REBOOK
DATABASE: PAR_UPS16_QRA
DYNAMIC PARAMETERS: ACCT_MTH: ACCOUNTING MONTH. ALWAYS USE FIRST DATE OF MONTH
*/


DECLARE @ACCT_MTH DATETIME 

--SET PARAMETERS HERE
SET @ACCT_MTH = '3/1/2019' 

SELECT RVNU_RUN_ID, 
       A.PROP_NO, 
       PROP_NM, 
       Sum([DOLLAR IMPACT])                                                AS 
       [DOLLAR IMPACT], 
       Sum(BOE)                                                            AS 
       [BOE IMPACT], 
       Round(Sum(BOE) / Datediff(DAY, @ACCT_MTH, Dateadd(MONTH, 1, @ACCT_MTH)), 2) AS 
       [BOE PER DAY], 
       CASE 
         WHEN Min([OLD TIER]) <> Max([NEW TIER]) THEN 
         CONCAT('TIER CHANGE FROM ', Min([OLD TIER]), ' TO ', Max([NEW TIER])) 
         WHEN Min([OLD TIER]) = Max([NEW TIER]) 
              AND [PPN TYPE] = 'RV36' THEN 'PURCHASER ADJUSTMENT' 
         WHEN Min([OLD TIER]) = Max([NEW TIER]) 
              AND [PPN TYPE] LIKE 'LD48' THEN 'TEMP TO PERM' 
         WHEN Min([OLD TIER]) = Max([NEW TIER]) 
              AND [PPN TYPE] LIKE 'LD%' THEN 'OWNER LEVEL DO ADJ' 
         ELSE 'UNKNOWN' 
       END 
       COMMENT 
FROM   (SELECT RVNU_RUN_ID, 
               PROP_NO, 
               OWNR_BA_NO, 
               Sum(ACT_OWNR_TRANS_AMT) AS [DOLLAR IMPACT], 
               CASE 
                 WHEN MAJ_PROD_CD = '200' THEN Sum(ACT_OWNR_GRS_VOL) / 6 
                 ELSE Sum(ACT_OWNR_GRS_VOL) 
               END                     AS BOE, 
               Min(TIER)               [OLD TIER], 
               Max(TIER)               [NEW TIER], 
               Max(PPN_RSN_CD)         [PPN TYPE] 
        FROM   RTRN_RD_OWNR_LVL_PSUM_HIST 
        WHERE  ACCTG_MTH = @ACCT_MTH
               AND PRDN_DT < Dateadd(MONTH, -1, @ACCT_MTH) 
               AND OWNR_BA_NO IN ( '3552', '12578' ) 
               AND MAJ_PROD_CD IN ( '100', '200', '400' ) 
        GROUP  BY OWNR_BA_NO, 
                  RVNU_RUN_ID, 
                  PROP_NO, 
                  MAJ_PROD_CD) A 
       LEFT JOIN GONL_PROP P 
              ON P.PROP_NO = A.PROP_NO 
WHERE  PROP_OPER_FL = 'Y' 
GROUP  BY RVNU_RUN_ID, 
          A.PROP_NO, 
          PROP_NM, 
          [PPN TYPE] 
ORDER  BY RVNU_RUN_ID 
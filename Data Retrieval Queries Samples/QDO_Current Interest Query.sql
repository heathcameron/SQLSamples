/*
*****READ ME*****
DESCRIPTION: BRINGS BACK CURRENT JIB AND REVENUE INTEREST FOR ALL WELLS
DATABASE: PAR_UPS16_QRA
DYNAMIC PARAMETERS:NONE
*/


SELECT COALESCE(REV.PROP_NO, JIB.PROP_NO)		 AS [PROPERTY NUMBER], 
       COALESCE(REV.PROP_NM, JIB.PROP_NM)		 AS [PROPERTY NAME], 
       REV.TIER                                  AS [REVENUE TIER], 
       Sum([REV NON-WI NRI])                     AS [REV NON-WI NRI], 
       Sum([REV WI NRI])                         AS [REV WI NRI], 
       Sum([REV NON-WI NRI]) + Sum([REV WI NRI]) AS [TOTAL REVENUE NRI], 
       JIB.TIER                                  AS [JIB TIER], 
       [JIB WI INTEREST] 
FROM   (SELECT D.PROP_NO, 
               PROP_NM, 
               BA_NO, 
               INT_TYPE_CD, 
               D.TIER, 
               D.DO_MAJ_PROD_CD, 
               CASE 
                 WHEN INT_TYPE_CD <> 'WI' THEN Sum(NRI_DEC) 
                 ELSE 0 
               END AS [REV NON-WI NRI], 
               CASE 
                 WHEN INT_TYPE_CD = 'WI' THEN Sum(NRI_DEC) 
                 ELSE 0 
               END AS [REV WI NRI] 
        FROM   DONL_DO_DETAIL D 
               INNER JOIN (SELECT DISTINCT PROP_NO, 
                                           TIER, 
                                           DO_TYPE_CD, 
                                           DO_MAJ_PROD_CD, 
                                           COMPL_NO 
                           FROM   PXRF_WELL_COMPL_PROP_DO_QRA 
                           WHERE  COMPL_NO = '1' 
                                  AND EFF_DT_TO = '12/31/9999') X 
                       ON X.PROP_NO = D.PROP_NO 
                          AND X.TIER = D.TIER 
                          AND X.DO_TYPE_CD = D.DO_TYPE_CD 
                          AND X.DO_MAJ_PROD_CD = D.DO_MAJ_PROD_CD 
               LEFT JOIN GONL_PROP P 
                      ON P.PROP_NO = D.PROP_NO 
        WHERE  D.EFF_DT_TO = '12/31/9999' 
               AND X.COMPL_NO = '1' 
               AND BA_NO IN ( '3552', '12578', '34933' ) 
               AND D.DO_TYPE_CD = 'REV' 
               AND ( D.DO_MAJ_PROD_CD = 'ALL' 
                      OR ( D.PROP_NO = '499465' 
                           AND D.DO_MAJ_PROD_CD = '100' ) ) 
        GROUP  BY D.PROP_NO, 
                  PROP_NM, 
                  BA_NO, 
                  INT_TYPE_CD, 
                  D.TIER, 
                  D.DO_MAJ_PROD_CD) REV 
       FULL OUTER JOIN (SELECT D.PROP_NO, 
                               PROP_NM, 
                               BA_NO, 
                               INT_TYPE_CD, 
                               D.TIER, 
                               D.DO_MAJ_PROD_CD, 
                               Sum(NRI_DEC) [JIB WI INTEREST] 
                        FROM   DONL_DO_DETAIL D 
                               LEFT JOIN DONL_DO_HDR HDR 
                                      ON HDR.PROP_NO = D.PROP_NO 
                                         AND HDR.DO_TYPE_CD = D.DO_TYPE_CD 
                                         AND HDR.DO_MAJ_PROD_CD = 
                                             D.DO_MAJ_PROD_CD 
                                         AND HDR.TIER = D.TIER 
                               LEFT JOIN GONL_PROP P 
                                      ON P.PROP_NO = D.PROP_NO 
                        WHERE  D.EFF_DT_TO = '12/31/9999' 
                               AND JIB_BASE_FL = 'Y' 
                               AND BA_NO IN ( '3552', '12578', '34933' ) 
                               AND D.DO_TYPE_CD = 'JIB' 
                        GROUP  BY D.PROP_NO, 
                                  PROP_NM, 
                                  BA_NO, 
                                  INT_TYPE_CD, 
                                  D.TIER, 
                                  D.DO_MAJ_PROD_CD) JIB 
                    ON JIB.PROP_NO = REV.PROP_NO 
GROUP  BY COALESCE(REV.PROP_NO, JIB.PROP_NO), 
          COALESCE(REV.PROP_NM, JIB.PROP_NM), 
          REV.TIER, 
          JIB.TIER, 
          [JIB WI INTEREST] 
ORDER  BY [REV WI NRI] DESC, 
          [JIB WI INTEREST] DESC 
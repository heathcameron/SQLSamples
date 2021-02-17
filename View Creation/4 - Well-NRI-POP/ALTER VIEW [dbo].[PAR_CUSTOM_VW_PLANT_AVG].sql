USE [QAS_PRD_PAR_CUSTOM]
GO

/****** Object:  View [dbo].[PAR_CUSTOM_VW_PLANT_AVG]    Script Date: 8/22/2019 10:18:40 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


ALTER VIEW [dbo].[PAR_CUSTOM_VW_PLANT_AVG] AS 

SELECT CASE 
         WHEN ATTR_TYPE_VALUE_DESCR = 'HORIZONTAL' THEN PLANT_NAME 
         ELSE 'VERTICAL' 
       END                                 AS PLANT_NAME, 
       ATTR_TYPE_VALUE_DESCR               AS ORIENTATION, 
       Sum(ACT_MCF_VOL) / Sum(CASE 
                                WHEN PLNT_INLET_MKT_SHR_VOL > 0 THEN 
                                PLNT_INLET_MKT_SHR_VOL 
                                ELSE 0 
                              END)         AS [AVG_SHRINK_FACTOR], 
       Sum(ACT_BBL_VOL) / ( Sum(CASE 
                                  WHEN PLNT_INLET_MKT_SHR_VOL > 0 THEN 
                                  PLNT_INLET_MKT_SHR_VOL 
                                  ELSE 0 
                                END) / 6 ) AS [AVG_YIELD_FACTOR], 
	    (Sum(ACT_BBL_VOL)*1000) / ( Sum(CASE 
                                  WHEN PLNT_INLET_MKT_SHR_VOL > 0 THEN 
                                  PLNT_INLET_MKT_SHR_VOL 
                                  ELSE 0 
                                END)) AS AVG_YIELD_PER_BBL, 
       Count(DISTINCT F.PROP_NO)           AS WELL_COUNT 
FROM   PAR_UPS16_QRA.dbo.TONL_TAX_INPUT F 
       LEFT JOIN PAR_POP_LIST L 
              ON L.PROP_NO = F.PROP_NO 
       LEFT JOIN PAR_UPS16_QRA.dbo.GONL_PROP P 
              ON L.PROP_NO = P.PROP_NO 
       LEFT JOIN PAR_UPS16_QRA.dbo.GONL_PROP_ATTR PA 
              ON P.PROP_NO = PA.PROP_NO 
                 AND ATTR_TYPE_CD = 'WOR' 
       INNER JOIN PAR_UPS16_QRA.dbo.SXREF_ATTR_TYPE_VAL X 
               ON PA.ATTR_VALUE = X.ATTR_TYPE_VALUE 
                  AND PA.ATTR_TYPE_CD = X.ATTR_TYPE_CD 
WHERE  MAJ_PROD_CD IN ( '200', '400' ) 
       AND SEV_TAX_TYPE_CD = 'PR' 
       AND PRDN_DT = (SELECT PAR_GAS_MONTH 
                      FROM   [PAR_GAS_MONTH]) 
GROUP  BY CASE 
            WHEN ATTR_TYPE_VALUE_DESCR = 'HORIZONTAL' THEN PLANT_NAME 
            ELSE 'VERTICAL' 
          END, 
          ATTR_TYPE_VALUE_DESCR 
GO



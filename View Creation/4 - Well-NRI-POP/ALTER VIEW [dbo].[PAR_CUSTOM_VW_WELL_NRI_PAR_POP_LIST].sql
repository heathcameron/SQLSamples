USE [QAS_PRD_PAR_CUSTOM]
GO

/****** Object:  View [dbo].[PAR_CUSTOM_VW_WELL_NRI_PAR_POP_LIST]    Script Date: 8/26/2019 5:06:38 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--DROP VIEW VW_WELL_NRI_POP_LIST
ALTER VIEW [dbo].[PAR_CUSTOM_VW_WELL_NRI_PAR_POP_LIST] AS 

SELECT L.PROP_NO, 
       PROP_NM, 
       ATTR_TYPE_VALUE_DESCR AS ORIENTATION, 
       L.PLANT_NAME, 
       PURCHASER_NUMBER, 
       CASE 
         WHEN USE_MANUAL_NRI = 'Y' THEN 'MANUAL' 
         WHEN COALESCE(LP.ESTIMATE_FL, PEM.ESTIMATE_FL) = 'Y' THEN 'ESTIMATE' 
         ELSE 'QDO' 
       END                   AS NRI_USED, 
       CASE 
         WHEN USE_MANUAL_NRI = 'Y' THEN COALESCE(LP_NRI_MANUAL, 0) 
         ELSE COALESCE(LP_NRI, 0) 
       END                   AS [LP INTEREST], 
       CASE 
         WHEN USE_MANUAL_NRI = 'Y' THEN COALESCE(PEM_NRI_MANUAL, 0) 
         ELSE COALESCE(PEM_NRI, 0) 
       END                   AS [PEM INTEREST], 
		COALESCE(POP,POP_MANUAL) AS POP, 
       CASE 
         WHEN WL.[SHRINK_FACTOR] IS NOT NULL 
              AND WL.[SHRINK_FACTOR] <> 0 THEN WL.[SHRINK_FACTOR] 
         WHEN ATTR_TYPE_VALUE_DESCR = 'VERTICAL' THEN VERT_AVG.AVG_SHRINK_FACTOR 
         ELSE AV.AVG_SHRINK_FACTOR 
       END                   AS [SHRINK_FACTOR], 
	      CASE 
         WHEN WL.[YIELD / BBL]   IS NOT NULL 
              AND WL.[YIELD / BBL]   <> 0 THEN WL.[YIELD / BBL]  
         WHEN ATTR_TYPE_VALUE_DESCR = 'VERTICAL' THEN VERT_AVG.AVG_YIELD_PER_BBL
         ELSE AV.AVG_YIELD_PER_BBL
       END                   AS [YIELD / BBL], 
       CASE 
         WHEN WL.[YIELD_FACTOR] IS NOT NULL 
              AND WL.[YIELD_FACTOR] <> 0 THEN WL.[YIELD_FACTOR] 
         WHEN ATTR_TYPE_VALUE_DESCR = 'VERTICAL' THEN VERT_AVG.AVG_YIELD_FACTOR 
         ELSE AV.AVG_YIELD_FACTOR 
       END                   AS [YIELD_FACTOR], 
       CASE 
         WHEN [SHRINK_FACTOR] IS NULL 
               OR [SHRINK_FACTOR] = 0 
               OR [YIELD_FACTOR] IS NULL 
               OR [YIELD_FACTOR] = 0 THEN 'Y' 
         ELSE 'N' 
       END                   AVERAGES_USED, 
       CASE WHEN POP IS NULL THEN 'Y' ELSE 'N' END  AS OVERRIDE_POP_USED 
FROM   POP_LIST L 
       LEFT JOIN PAR_CUSTOM_VW_GONL_PROP P 
              ON L.PROP_NO = P.PROP_NO 
       LEFT JOIN PAR_UPS16_QRA.dbo.GONL_PROP_ATTR PA 
              ON P.PROP_NO = PA.PROP_NO 
                 AND ATTR_TYPE_CD = 'WOR' 
       INNER JOIN PAR_UPS16_QRA.dbo.SXREF_ATTR_TYPE_VAL X 
               ON PA.ATTR_VALUE = X.ATTR_TYPE_VALUE 
                  AND PA.ATTR_TYPE_CD = X.ATTR_TYPE_CD 
       LEFT JOIN (SELECT COALESCE(Sum([DOI_INTEREST]), 0) AS LP_NRI, 
                         PROP_NO, 
                         ESTIMATE_FL 
                  FROM   PAR_CUSTOM_VW_INTERNAL_REVENUE_INTEREST 
                  WHERE  BA_NO = '3552' 
                  GROUP  BY PROP_NO, 
                            ESTIMATE_FL) LP 
              ON LP.PROP_NO = P.PROP_NO 
       LEFT JOIN (SELECT COALESCE(Sum([DOI_INTEREST]), 0) AS PEM_NRI, 
                         PROP_NO, 
                         ESTIMATE_FL 
                  FROM   PAR_CUSTOM_VW_INTERNAL_REVENUE_INTEREST 
                  WHERE  BA_NO = '12578' 
                  GROUP  BY PROP_NO, 
                            ESTIMATE_FL) PEM 
              ON PEM.PROP_NO = P.PROP_NO 
       LEFT JOIN [PAR_CUSTOM_VW_WELL_LEVEL_GAS_VOLUME_DATA] WL 
              ON WL.PROP_NO = P.PROP_NO 
       LEFT JOIN [PAR_CUSTOM_VW_PLANT_AVG] AV 
              ON L.PLANT_NAME = AV.PLANT_NAME 
                 AND ORIENTATION = ATTR_TYPE_VALUE_DESCR 
       LEFT JOIN [PAR_CUSTOM_VW_PLANT_AVG] VERT_AVG 
              ON VERT_AVG.PLANT_NAME = 'VERTICAL' 
       LEFT JOIN GAS_PLANTS GP 
              ON L.PLANT_NAME = GP.GAS_PLANT 

GO



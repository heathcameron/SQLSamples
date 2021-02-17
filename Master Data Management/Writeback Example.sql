USE [QuorumAcct_Integration_Prod]
GO

/****** Object:  StoredProcedure [Enerhub].[AL_Well_Writeback]    Script Date: 1/4/2021 11:19:14 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:      Stonebridge
-- Description: Stored procedure to insert or update WELL data INTO Quorum
-- Created:     12/05/19 - DZ
-- Updated :    02/11/20 - GK- Added the logic to generate the CMNT_SEQ_NO using the [PAR_UPS16_QRA].[dbo].QARCH_GETNEXTSEQ stored procedure #20491.
-- =============================================

CREATE PROCEDURE [Enerhub].[AL_Well_Writeback]
  -- Header Attributes
   @WELL_GOVERNMENT_ID                  NVARCHAR(255) = NULL
  ,@WELL_NAME                           NVARCHAR(255) = NULL
  ,@WELL_NAME_FKA                       NVARCHAR(255) = NULL
  ,@UWI                                 NVARCHAR(255) = NULL
  ,@PROPERTY_NUMBER                     NVARCHAR(255) = NULL

  -- Master Attributes
  ,@ABSTRACT_SHL                        NVARCHAR(255) = NULL
  ,@BASIN                               NVARCHAR(255) = NULL
  ,@BATTERY                             NVARCHAR(255) = NULL
  ,@BLOCK_SHL                           NVARCHAR(255) = NULL
  ,@COORDINATE_DATUM_SHL                NVARCHAR(255) = NULL
  ,@COORDINATE_SOURCE_SHL               NVARCHAR(255) = NULL
  ,@COUNTY_SHL                          NVARCHAR(255) = NULL
  ,@DIVESTITURE                         NVARCHAR(255) = NULL
  ,@DRILLING_PERMIT_NUMBER              NVARCHAR(255) = NULL
  ,@DRILLING_PRODUCTION_START_DATE      DATETIME      = NULL
  ,@ELEVATION_GROUND                    NVARCHAR(255) = NULL
  ,@ESI_ID                              NVARCHAR(255) = NULL
  ,@FOREMAN_NAME                        NVARCHAR(255) = NULL
  ,@INTERMEDIATE_END_DATE               DATETIME      = NULL
  ,@INTERMEDIATE_START_DATE             DATETIME      = NULL
  ,@LATITUDE_SHL                        DECIMAL(38,8) = NULL
  ,@LONGITUDE_SHL                       DECIMAL(38,8) = NULL
  ,@LATITUDE_BHL                        DECIMAL(38,8) = NULL
  ,@LONGITUDE_BHL                       DECIMAL(38,8) = NULL
  ,@OPERATOR                            NVARCHAR(255) = NULL
  ,@PA_DATE                             DATETIME      = NULL
  ,@PAD_NAME                            NVARCHAR(255) = NULL
  ,@PERMIT_APPROVED_DATE                DATETIME      = NULL
  ,@PERMIT_SUBMITTED_DATE               DATETIME      = NULL
  ,@PILOT_END_DATE                      DATETIME      = NULL
  ,@PILOT_START_DATE                    DATETIME      = NULL
  ,@PRODUCING_METHOD                    NVARCHAR(255) = NULL
  ,@PUMPER_NAME                         NVARCHAR(255) = NULL
  ,@REGULATORY_FIELD                    NVARCHAR(255) = NULL
  ,@REGULATORY_LEASE_NAME               NVARCHAR(255) = NULL
  ,@RIG_RELEASE_DATE                    DATETIME      = NULL
  ,@RRC_LEASE_ID                        NVARCHAR(255) = NULL
  ,@SECTION_SHL                         NVARCHAR(255) = NULL
  ,@STATE_SHL                           NVARCHAR(255) = NULL
  ,@SURFACE_END_DATE_SHL                DATETIME      = NULL
  ,@SURFACE_SPUD_DATE_SHL               NVARCHAR(100) = NULL
  ,@SURVEY_NAME_SHL                     NVARCHAR(255) = NULL
  ,@TOTAL_MEASURED_DEPTH                NVARCHAR(255) = NULL
  ,@TOWNSHIP_SHL                        NVARCHAR(255) = NULL
  ,@WELL_CLASS_NAME                     NVARCHAR(255) = NULL
  ,@WELL_ORIENTATION                    NVARCHAR(255) = NULL
  ,@WELL_STATUS                         NVARCHAR(255) = NULL
  ,@WELL_STATUS_EFFECTIVE_DATE          DATETIME = NULL
  ,@ACQ_FLAG                            NCHAR(1)      = NULL

  -- System Attributes
  ,@TransactionUser                     VARCHAR(255)  = NULL --Save as user

  -- Error Logging
  ,@WELLId                              VARCHAR(255)  = NULL -- Used for Error Logging, WelldId
  ,@Well_Version_LoadId                 VARCHAR(255)  = NULL -- Used for Error Logging, Well_Version_LoadId
  ,@ErrorMessage                        NVARCHAR(MAX) = NULL
  ,@ErrorSeverity                       INT           = NULL
  ,@ErrorState                          INT           = NULL

  --Identifier Attributes
  ,@WELL_LEVEL_TYPE                     NVARCHAR(20)  = NULL
  ,@SOURCE                              NVARCHAR(30)  = NULL
  ,@SOURCE_REFERENCE_NUM                NVARCHAR(50)  = NULL
  ,@ROW_CHANGED_BY                      NVARCHAR(30)  = NULL
  ,@ROW_CHANGED_DATE                    DATETIME      = NULL
  ,@ROW_CREATED_BY                      NVARCHAR(30)  = NULL
  ,@ROW_CREATED_DATE                    DATETIME      = NULL

AS
IF @ACQ_FLAG = 'Y'
BEGIN 
  --SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.--
  SET NOCOUNT ON

  BEGIN TRY

    BEGIN  --GLOBAL VARIABLES AND CONSTANTS  
    --**************************************************************
      DECLARE @ERROR_MESSAGE           NVARCHAR(MAX) = NULL;
      DECLARE @ERROR_SEVERITY          INT           = NULL;
      DECLARE @ERROR_STATE             INT           = NULL;
	  DECLARE @ERROR_STATUS_DUPLICATE  BIT			 = 0;
      DECLARE @WELL_CLASS_CHANGED      BIT           = 0;
      DECLARE @WELL_STATUS_CHANGED     BIT           = 0;
      DECLARE @CURRENT_MONTH_START     DATETIME      = CAST(GETDATE() - DAY(GETDATE()) + 1 AS DATE); --*****FIRST DAY OF THE CURRENT MONTH  
      DECLARE @PREVIOUS_MONTH_END      DATETIME      = DATEADD(s, -1, DATEADD(mm, DATEDIFF(m, 0, GETDATE()), 0)); --*****LAST DAY OF THE PREVIOUS MONTH
      DECLARE @YESTERDAY               DATETIME      = CAST(DATEADD(DAY, DATEDIFF(DAY, 1, GETDATE()), 0) AS DATE); --*****YESTERDAY'S DATE
      DECLARE @BASIN_ORG_TYPE          INT           = ISNULL((SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCODE_ORG_TYPE_V2 WHERE ORG_TYPE_CD = 'BSN'), 1);
      DECLARE @BATTERY_ORG_TYPE        INT           = ISNULL((SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCODE_ORG_TYPE_V2 WHERE ORG_TYPE_CD = 'BAT'), 2);
      DECLARE @CC_ORG_TYPE             INT           = ISNULL((SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCODE_ORG_TYPE_V2 WHERE ORG_TYPE_CD = 'CC'), 3);
      DECLARE @PAD_ORG_TYPE            INT           = ISNULL((SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCODE_ORG_TYPE_V2 WHERE ORG_TYPE_CD = 'PAD'), 4);
      DECLARE @ORG_PROFILE_ID          INT           = ISNULL((SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_PROFILE_V2 WHERE PROFILE_CD = 'STD'), 94);
      DECLARE @EXISTING_TONL_TX_EFF_TO DATETIME      = NULL;
      DECLARE @GONL_PROP_UWI           NVARCHAR(50)  = NULL;
	  DECLARE @WELL_STATUS_EFFICTIVE_DATE_TO	DATETIME = NULL;
      --DECLARE @STATUS_NEW_START_DATE        AS DATETIME      = NULL;
  
      SET @WELL_LEVEL_TYPE = 'WELL';
    --**************************************************************
    END

    BEGIN  --DETERMINE WHICH ID'S ARE ALREADY PRESENT IN THE SYSTEM  
    --**************************************************************
      DECLARE @EXISTING_WELL_ID AS VARCHAR(50) = (SELECT TOP 1 A.ORG_CD AS PROP_NO FROM [PAR_UPS16_QRA].[dbo].[SCTRL_ORG_V2] A INNER JOIN [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR B ON A.ORG_CD = B.PROP_NO WHERE A.ID_ORG_TYPE = @CC_ORG_TYPE AND B.ATTR_TYPE_CD = 'WI' AND B.OPER_BUS_SEG_CD = 'PAR' AND B.ATTR_VALUE = @UWI);
      DECLARE @EXISTING_BASIN_ID AS VARCHAR(50) = (SELECT TOP 1 ATTR_VALUE FROM [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR WHERE ATTR_TYPE_CD = 'BSN' AND OPER_BUS_SEG_CD = 'PAR' AND PROP_NO = @EXISTING_WELL_ID);
      DECLARE @EXISTING_BASIN_XREF AS VARCHAR(50) = (SELECT TOP 1 ATTR_TYPE_VALUE FROM PAR_UPS16_ESUITE.dbo.SXREF_ATTR_TYPE_VAL WHERE UPPER(ATTR_TYPE_VALUE_DESCR) = UPPER(@BASIN) AND ATTR_GRP_CD = 'CC' AND ATTR_TYPE_CD = 'BSN');
      DECLARE @EXISTING_ORIENTATION_ID AS VARCHAR(50) = (SELECT TOP 1 ATTR_VALUE FROM [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR WHERE ATTR_TYPE_CD = 'WOR' AND OPER_BUS_SEG_CD = 'PAR' AND PROP_NO = @EXISTING_WELL_ID);
      DECLARE @EXISTING_ORIENTATION_XREF AS VARCHAR(50) = (SELECT TOP 1 ATTR_TYPE_VALUE FROM PAR_UPS16_ESUITE.dbo.SXREF_ATTR_TYPE_VAL WHERE UPPER(ATTR_TYPE_VALUE_DESCR) = UPPER(@WELL_ORIENTATION) AND ATTR_GRP_CD = 'CC' AND ATTR_TYPE_CD = 'WOR');
      DECLARE @EXISTING_DIVESTITURE_ID AS VARCHAR(50) = (SELECT TOP 1 ATTR_VALUE FROM [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR WHERE ATTR_TYPE_CD = 'DIV' AND OPER_BUS_SEG_CD = 'PAR' AND PROP_NO = @EXISTING_WELL_ID);
      DECLARE @EXISTING_DIVESTITURE_XREF AS VARCHAR(50) = (SELECT TOP 1 ATTR_TYPE_VALUE FROM PAR_UPS16_ESUITE.dbo.SXREF_ATTR_TYPE_VAL WHERE UPPER(ATTR_TYPE_VALUE_DESCR) = UPPER(@DIVESTITURE) AND ATTR_GRP_CD = 'CC' AND ATTR_TYPE_CD = 'DIV');
      DECLARE @EXISTING_ESI_ID AS VARCHAR(50) = (SELECT TOP 1 ATTR_VALUE FROM [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR WHERE ATTR_TYPE_CD = 'ESI' AND OPER_BUS_SEG_CD = 'PAR' AND PROP_NO = @EXISTING_WELL_ID);
      DECLARE @EXISTING_FOREMAN_ID AS VARCHAR(50) = (SELECT TOP 1 ATTR_VALUE FROM [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR WHERE ATTR_TYPE_CD = 'FOR' AND OPER_BUS_SEG_CD = 'PAR' AND PROP_NO = @EXISTING_WELL_ID);
      DECLARE @EXISTING_PUMPER_ID AS VARCHAR(50) = (SELECT TOP 1 ATTR_VALUE FROM [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR WHERE ATTR_TYPE_CD = 'PUM' AND OPER_BUS_SEG_CD = 'PAR' AND PROP_NO = @EXISTING_WELL_ID);
      DECLARE @EXISTING_PONL_WELL_ID AS VARCHAR(50) = (SELECT TOP 1 WELL_NO FROM [PAR_UPS16_QRA].[dbo].PONL_WELL WHERE OPER_BUS_SEG_CD = 'PAR' AND WELL_NO = @EXISTING_WELL_ID);
      DECLARE @EXISTING_GONL_PROP_EFF_ID AS VARCHAR(50) = (SELECT TOP 1 PROP_NO FROM [PAR_UPS16_QRA].[dbo].GONL_PROP_EFF_DT WHERE OPER_BUS_SEG_CD = 'PAR' AND PROP_NO = @EXISTING_WELL_ID AND GETDATE() BETWEEN EFF_DT_FROM AND EFF_DT_TO);
      DECLARE @EXISTING_PONL_WELL_COMPL_ID AS VARCHAR(50) = (SELECT TOP 1 WELL_NO FROM [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL WHERE OPER_BUS_SEG_CD = 'PAR' AND WELL_NO = @EXISTING_WELL_ID AND COMPL_NO = 1);
      DECLARE @EXISTING_PONL_WELL_COMPL_EFF_ID AS VARCHAR(50) = (SELECT TOP 1 WELL_NO FROM [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT WHERE OPER_BUS_SEG_CD = 'PAR' AND WELL_NO = @EXISTING_WELL_ID AND COMPL_NO = 1 AND GETDATE() BETWEEN EFF_DT_FROM AND EFF_DT_TO);
      DECLARE @EXISTING_WELL_CLASS_CD AS CHAR(1) = (SELECT TOP 1 WELL_CLASS_CD FROM [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT WHERE OPER_BUS_SEG_CD = 'PAR' AND WELL_NO = @EXISTING_WELL_ID AND COMPL_NO = 1 AND GETDATE() BETWEEN EFF_DT_FROM AND EFF_DT_TO);
      DECLARE @EXISTING_WELL_CLASS_EFF_DT_FROM AS DATETIME = (SELECT TOP 1 EFF_DT_FROM FROM [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT WHERE OPER_BUS_SEG_CD = 'PAR' AND WELL_NO = @EXISTING_WELL_ID AND COMPL_NO = 1 AND GETDATE() BETWEEN EFF_DT_FROM AND EFF_DT_TO);
      DECLARE @EXISTING_TONL_TX_ID AS BIGINT = (SELECT TOP 1 TX_CPA_SEQ_NO FROM [PAR_UPS16_QRA].[dbo].TONL_TX_CPA_MST WHERE OPER_BUS_SEG_CD = 'PAR' AND EFF_DT_TO > GETDATE() AND WELL_NO = @EXISTING_WELL_ID AND EFF_DT_FROM = (SELECT MAX(EFF_DT_FROM) FROM [PAR_UPS16_QRA].[dbo].TONL_TX_CPA_MST WHERE OPER_BUS_SEG_CD = 'PAR' AND EFF_DT_TO > GETDATE() AND WELL_NO = @EXISTING_WELL_ID));
      DECLARE @EXISTING_LEASE_ID AS VARCHAR(6) = (SELECT TOP 1 LSE_NO FROM [PAR_UPS16_QRA].[dbo].TONL_TX_CPA_MST WHERE TX_CPA_SEQ_NO = @EXISTING_TONL_TX_ID);  --STORE THE CURRENT LEASE_ID FOR LATER
      DECLARE @EXISTING_DRL_PMT_NO AS VARCHAR(6) = (SELECT TOP 1 DRL_PRMT_NO FROM [PAR_UPS16_QRA].[dbo].TONL_TX_CPA_MST WHERE TX_CPA_SEQ_NO = @EXISTING_TONL_TX_ID);  --STORE THE CURRENT DRL_PMT_NO FOR LATER
      DECLARE @EXISTING_WELL_STATUS_ID AS VARCHAR(4) = (SELECT TOP 1 WELL_STAT_CD FROM [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT WHERE OPER_BUS_SEG_CD = 'PAR' AND WELL_NO = @EXISTING_WELL_ID AND COMPL_NO = 1 AND EFF_DT_TO = '9999-12-31 00:00:00.000'); 
      DECLARE @EXISTING_WELL_STATUS_NAME AS VARCHAR(255) = (SELECT TOP 1 WELL_STAT_DESCR FROM [PAR_UPS16_QRA].[dbo].[GCDE_WELL_STAT] WHERE WELL_STAT_CD = @EXISTING_WELL_STATUS_ID);
      DECLARE @EXISTING_WELL_STATUS_LATEST_ID AS VARCHAR(4) = (SELECT TOP 1 WELL_STAT_CD FROM [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT WHERE OPER_BUS_SEG_CD = 'PAR' AND WELL_NO = @EXISTING_WELL_ID AND COMPL_NO = 1 AND UPDT_DT = (SELECT MAX(UPDT_DT) FROM [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT WHERE OPER_BUS_SEG_CD = 'PAR' AND WELL_NO = @EXISTING_WELL_ID AND COMPL_NO = 1));
	  DECLARE @EXISTING_WELL_STATUS_LATEST_NAME AS VARCHAR(255) = (SELECT TOP 1 WELL_STAT_DESCR FROM [PAR_UPS16_QRA].[dbo].[GCDE_WELL_STAT] WHERE WELL_STAT_CD = @EXISTING_WELL_STATUS_LATEST_ID);
      DECLARE @EXISTING_STATE_ID AS VARCHAR(3) = ISNULL((SELECT TOP 1 API_STATE_CD FROM PAR_UPS16_ESUITE.dbo.SCODE_STATE WHERE COUNTRY_CD = 'US' AND STATE_ABBR = @STATE_SHL), '000');
      DECLARE @EXISTING_COUNTY_ID AS VARCHAR(3) = ISNULL((SELECT TOP 1 CNTY_CD FROM PAR_UPS16_QRA.dbo.GCDE_CNTY WHERE ST_CD = @EXISTING_STATE_ID AND CTRY_CD = 'US' AND UPPER(CNTY_DESCR) = @COUNTY_SHL), '000');
      DECLARE @EXISTING_OPERATOR_ID AS NVARCHAR(6) = (SELECT BA_NO FROM [PAR_UPS16_QRA].[dbo].SCTRL_BA_ENTITY WHERE BA_NM1 = 'PARSLEY ENERGY OPERATIONS, LLC');
      DECLARE @EXISTING_WELL_ORG_ID AS INT = (SELECT TOP 1 ORG_ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_CD = @PROPERTY_NUMBER);
      DECLARE @EXISTING_WELL_SCTRL_ID AS INT = (SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_CD = @PROPERTY_NUMBER);
      DECLARE @EXISTING_COST_CENTER_ID AS INT = (SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_COST_CNTR WHERE OPER_BUS_SEG_CD = 'PAR' AND COST_CNTR_CD = @PROPERTY_NUMBER);
      
      DECLARE @EXISTING_PAD_ORG_ID AS INT = (SELECT TOP 1 D.ID 
                            FROM [PAR_UPS16_ESUITE].[dbo].SCTRL_ORG_V2 B LEFT JOIN --WELL
                                [PAR_UPS16_ESUITE].[dbo].SCTRL_ORG_HIERARCHY_V2 C ON C.ID_ORG = B.ID LEFT JOIN 
                                [PAR_UPS16_ESUITE].[dbo].SCTRL_ORG_V2 D ON D.ID = C.ID_PARENTORG --PAD
                            WHERE B.ORG_CD = @PROPERTY_NUMBER AND D.ID_ORG_TYPE = @PAD_ORG_TYPE); 
      DECLARE @PAD_SCTRL_ID AS INT = (SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_NAME = @PAD_NAME AND ID_ORG_TYPE = @PAD_ORG_TYPE);
      DECLARE @EXISTING_WELL_PAD_ASSIGNMENT_ID AS INT = (SELECT TOP 1 ID FROM [PAR_UPS16_ESUITE].[dbo].SCTRL_ORG_HIERARCHY_V2 WHERE (ID_PARENTORG = @EXISTING_PAD_ORG_ID OR ID_PARENTORG IS NULL) AND ID_ORG = @EXISTING_WELL_SCTRL_ID);
      
      
      DECLARE @EXISTING_BATTERY_ORG_ID AS INT = (SELECT TOP 1 D.ID 
                            FROM [PAR_UPS16_ESUITE].[dbo].SCTRL_ORG_V2 B LEFT JOIN --WELL
                                [PAR_UPS16_ESUITE].[dbo].SCTRL_ORG_HIERARCHY_V2 C ON C.ID_ORG = B.ID LEFT JOIN 
                                [PAR_UPS16_ESUITE].[dbo].SCTRL_ORG_V2 D ON D.ID = C.ID_PARENTORG --BATTERY
                            WHERE B.ORG_CD = @PROPERTY_NUMBER AND D.ID_ORG_TYPE = @BATTERY_ORG_TYPE);
      DECLARE @BATTERY_SCTRL_ID AS INT = (SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_NAME = ISNULL(@BATTERY, '') AND ID_ORG_TYPE = @BATTERY_ORG_TYPE);
      DECLARE @EXISTING_WELL_BATTERY_ASSIGNMENT_ID AS INT = (SELECT TOP 1 ID FROM [PAR_UPS16_ESUITE].[dbo].SCTRL_ORG_HIERARCHY_V2 WHERE (ID_PARENTORG = @EXISTING_BATTERY_ORG_ID OR ID_PARENTORG IS NULL) AND ID_ORG = @EXISTING_WELL_SCTRL_ID);
      
      DECLARE @EXISTING_CMNT_WELL_ID AS BIGINT = (SELECT TOP 1 CMNT_SEQ_NO FROM PAR_UPS16_QRA.dbo.GONL_PROP WHERE PROP_NO = @EXISTING_WELL_ID);  --GET THE CURRENT WELL'S COMMENT SEQUENCE NUMBER (FOR USE WITH WELL_NAME_FKA)
      DECLARE @EXISTING_CMNT_DETAIL_ID AS BIGINT = (SELECT TOP 1 CMNT_SEQ_NO FROM PAR_UPS16_QRA.dbo.GONL_CMNT_DETAIL WHERE CMNT_TYPE_CD = 'FKA' AND CMNT_GRP_CD = 'PR' AND CMNT_TBL_NM_ID = 'GONL_PROP' AND CMNT_SEQ_NO = @EXISTING_CMNT_WELL_ID);  --DETERMINE IF THE COMMENT EXISTS YET (FOR USE WITH WELL_NAME_FKA)
      DECLARE @EXISTING_CMNT_HEADER_ID AS BIGINT = (SELECT TOP 1 CMNT_SEQ_NO FROM PAR_UPS16_QRA.dbo.GONL_CMNT_HDR WHERE CMNT_GRP_CD = 'PR' AND CMNT_TBL_NM_ID = 'GONL_PROP' AND CMNT_SEQ_NO = @EXISTING_CMNT_DETAIL_ID);  --DETERMINE IF THE COMMENT HEADER RECORD EXISTS YET (FOR USE WITH WELL_NAME_FKA)
      DECLARE @EXISTING_TAX_DESK_ID AS VARCHAR(10) = (SELECT TOP 1 DESKID_NO FROM PAR_UPS16_QRA.dbo.GXRF_DESKID_TAX WHERE WELL_NO = @EXISTING_WELL_ID AND COMPL_NO = 1 AND OPER_BUS_SEG_CD = 'PAR');
    --**************************************************************
    END
    
    BEGIN  --CREATE NEW ID'S FOR USE IF NEEDED  
    --**************************************************************
      DECLARE @NEW_WELL_ID AS VARCHAR(50) = @PROPERTY_NUMBER;    
      DECLARE @NEW_COST_CENTER_ID AS INT = (SELECT MAX(ID_COST_CNTR) + 1 FROM [PAR_UPS16_QRA].[dbo].GONL_PROP);
      DECLARE @NEW_CMNT_HEADER_ID AS BIGINT 
      DECLARE @NEW_ORG_ID  AS INT = (SELECT MAX(ORG_ID) + 1 FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2);
      DECLARE @NEW_PAD_ORG_ID AS INT = NULL;  --POPULATE THIS BELOW, BECAUSE IT'S THE SAME QUERY AS @NEW_ORG_ID AND THE VALUE MAY HAVE INCREMENTED BY THE TIME WE CREATE A PAD
      DECLARE @NEW_PAD_CODE AS VARCHAR(9) = ISNULL((SELECT MAX(CAST(LEFT(ORG_CD, 5) AS INT)) + 1 FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ID_ORG_TYPE = @PAD_ORG_TYPE AND ISNUMERIC(LEFT(ORG_CD, 5)) = 1), '10000')
      DECLARE @NEW_WELL_STATUS_ID AS VARCHAR(4) = (SELECT TOP 1 WELL_STAT_CD FROM [PAR_UPS16_QRA].[dbo].[GCDE_WELL_STAT] WHERE WELL_STAT_DESCR = @WELL_STATUS);
    --**************************************************************
    END
    
    BEGIN  --ADJUST PARAMETER VALUES FOR SOURCE SYSTEM CONSUMPTION  
    --**************************************************************
      IF @BASIN IS NULL SET @BASIN = 'MIDLAND BASIN';
      IF @EXISTING_BASIN_XREF IS NULL SET @EXISTING_BASIN_XREF = 'MIDLAND';

      SET @REGULATORY_LEASE_NAME = ISNULL(@REGULATORY_LEASE_NAME, @WELL_NAME);

      IF @EXISTING_BATTERY_ORG_ID IS NULL
        SET @EXISTING_BATTERY_ORG_ID = CASE WHEN @BASIN = 'MIDLAND BASIN' THEN ISNULL((SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_NAME = 'MIDLAND PRE-DRILL BATTERY'), 124629)      --MIDLAND PRE-DRILL BATTERY ORG ID
                                          WHEN @BASIN = 'DELAWARE BASIN' THEN ISNULL((SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_NAME = 'DELAWARE PRE-DRILL BATTERY'), 124632)    --DELAWARE PRE-DRILL BATTERY ORG ID
                                          ELSE 119628                    --PARSLEY COMPANY LEVEL ORG ID
                                        END;
      --GET THE PROPER WELL CLASS CHARACTER FOR USE IN QUORUM
      SET @WELL_CLASS_NAME = CASE 
                    WHEN UPPER(@WELL_CLASS_NAME) IN ('O', 'OIL') THEN 'O'
                    WHEN UPPER(@WELL_CLASS_NAME) IN ('G', 'GAS') THEN 'G'
                    WHEN UPPER(@WELL_CLASS_NAME) IN ('D', 'SWD', 'SALT WATER DISPOSAL', 'DISPOSAL') THEN 'D'
                    ELSE 'U' END;
      
      --DETERMINE WHETHER OR NOT THE WELL CLASS HAS CHANGED (OIL/GAS/SWD/D)
      SET @WELL_CLASS_CHANGED = CASE WHEN (ISNULL(@EXISTING_WELL_CLASS_CD, 'U') <> ISNULL(@WELL_CLASS_NAME, 'U')) AND (@EXISTING_WELL_CLASS_CD IS NOT NULL) AND (@WELL_CLASS_NAME IS NOT NULL) THEN 1
                    ELSE 0 END;

      --DRILLING_PERMIT_NUMBER AND RRC_LEASE_ID MUST BE 6 DIGITS INCLUDING AN EXTRA ZERO
      SET @RRC_LEASE_ID = ISNULL(REPLACE(REPLACE(STR(@RRC_LEASE_ID,6,0),' ','0'), '000000', ''), '');
      SET @EXISTING_LEASE_ID = ISNULL(REPLACE(REPLACE(STR(@EXISTING_LEASE_ID,6,0),' ','0'), '000000', ''), '');
      SET @DRILLING_PERMIT_NUMBER = REPLACE(STR(@DRILLING_PERMIT_NUMBER,6,0),' ','0');
      SET @EXISTING_DRL_PMT_NO = REPLACE(STR(@EXISTING_DRL_PMT_NO,6,0),' ','0');

	  --DETERMINE WHETHER OR NOT THE WELL STATUS END DATE WAS SET INCORRECTLY, THERE SHOULD ALWAYS BE A DATE THAT ENDS WITH '9999-12-31 00:00:00.000'
	  IF @EXISTING_WELL_STATUS_ID IS NULL AND @EXISTING_WELL_STATUS_LATEST_ID IS NOT NULL 
		BEGIN
			SET @EXISTING_WELL_STATUS_ID = @EXISTING_WELL_STATUS_LATEST_ID
			SET @EXISTING_WELL_STATUS_NAME = @EXISTING_WELL_STATUS_LATEST_NAME
		END

      --DETERMINE WHETHER OR NOT THE WELL STATUS HAS CHANGED
      IF @EXISTING_WELL_STATUS_ID <> @NEW_WELL_STATUS_ID SET @WELL_STATUS_CHANGED = 1


      IF @WELL_STATUS_EFFECTIVE_DATE IS NULL SET @WELL_STATUS_EFFECTIVE_DATE = GETDATE()
	  
	  --SET THE EFFECTIVE DATE AS TODAY IF STATUS = CANCELLED
	  IF LEFT(@WELL_STATUS, 9) = 'CANCELLED' SET @WELL_STATUS_EFFECTIVE_DATE = GETDATE();

	  --FORMAT THE STATUS EFFECTIVE DATE FOR QUORUM CONSUMPTION
      SET @WELL_STATUS_EFFECTIVE_DATE = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, @WELL_STATUS_EFFECTIVE_DATE), @WELL_STATUS_EFFECTIVE_DATE)
      SET @WELL_STATUS_EFFICTIVE_DATE_TO = DATEADD(HOUR, -1, @WELL_STATUS_EFFECTIVE_DATE)
    --**************************************************************
    END

    BEGIN  --CREATE NEW COST CENTER IF NEEDED  
    --**************************************************************
      IF (@EXISTING_WELL_ID IS NULL) AND NOT EXISTS (SELECT 1 FROM [PAR_UPS16_QRA].[dbo].SCTRL_COST_CNTR WHERE ID NOT IN (SELECT ID_COST_CNTR FROM [PAR_UPS16_QRA].[dbo].GONL_PROP) AND ID = @NEW_COST_CENTER_ID) 
        BEGIN TRY
          BEGIN TRAN

          --***** CREATE A NEW COMPLETION / COST CENTER ORG
          INSERT INTO PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2
          (
            ACTIVE_IND 
            ,OPER_BUS_SEG_CD 
            ,ORG_NAME 
            ,ORG_CD 
            ,ID_ORG_TYPE
            ,CREATE_USER_ID
            ,CREATE_DT
            ,[USER_ID]
            ,UPDT_DT
            ,ORG_ID
          )
          VALUES
          (
            1
            ,'PAR'
            ,@WELL_NAME
            ,@PROPERTY_NUMBER
            ,@CC_ORG_TYPE
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
            ,@NEW_ORG_ID
          );

          --***** ASSIGN THE NEW ORG TO THE EXISTING HIERARCHY / BATTERY
          INSERT INTO PAR_UPS16_ESUITE.dbo.SCTRL_ORG_HIERARCHY_V2
          (  
            OPER_BUS_SEG_CD 
            ,ID_ORG 
            ,ID_ORG_PROFILE 
            ,ID_PARENTORG
            ,CREATE_USER_ID
            ,CREATE_DT
            ,[USER_ID]
            ,UPDT_DT
          )
          VALUES
          (
            'PAR'
            ,(SELECT ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_ID = @NEW_ORG_ID)
            ,@ORG_PROFILE_ID
            ,@EXISTING_BATTERY_ORG_ID
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

          --***** CREATE A COST CENTER AND ASSIGN IT TO THE NEW ORG
          INSERT INTO PAR_UPS16_ESUITE.dbo.SCTRL_COST_CNTR
          (
             ACTIVE_IND
             ,OPER_BUS_SEG_CD
             ,COST_CNTR_CD
             ,ID_COST_CNTR_TYPE   
             ,COST_CNTR_DESCR
             ,ID_ORG
             ,CREATE_USER_ID
             ,CREATE_DT
             ,[USER_ID]
             ,UPDT_DT
          )
          VALUES
          (
            1
            ,'PAR'
            ,@PROPERTY_NUMBER
            ,ISNULL((SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCODE_COST_CNTR_TYPE WHERE COST_CNTR_TYPE_DESCR = 'WELL COMPLETION'), 73)
            ,@WELL_NAME
            ,(SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_ID = @NEW_ORG_ID)
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );
          
          --App launches ORGCOSTGEN process which executes 2 SP:
          EXEC PAR_UPS16_ESUITE.dbo.USPS_LOAD_RPT_ORG_HIERARCHY 'EH_WELL_WB_SP', 'PAR';
          EXEC PAR_UPS16_ESUITE.dbo.USPS_LOAD_RPT_COST_CNTR 'EH_WELL_WB_SP', 'PAR';


          --***** LOG SUCCESS ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
          VALUES(1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum SCTRL_COST_CNTR Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI);
        COMMIT TRAN

        --***** COST CENTER WAS CREATED *****
        SET @EXISTING_WELL_ORG_ID = @NEW_ORG_ID;
        SET @EXISTING_WELL_SCTRL_ID = (SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_CD = @PROPERTY_NUMBER);
        SET @BATTERY_SCTRL_ID = @EXISTING_BATTERY_ORG_ID
      END TRY
      BEGIN CATCH
        ROLLBACK TRAN
          
        --***** COST CENTER WAS NOT CREATED *****
        SET @NEW_COST_CENTER_ID = NULL

        --***** LOG FAILURE ***** 
        INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
        VALUES(1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum SCTRL_COST_CNTR Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI);

        SELECT
          @ERROR_MESSAGE = ERROR_MESSAGE(),
          @ERROR_SEVERITY = ERROR_SEVERITY(),
          @ERROR_STATE = ERROR_STATE();

        RAISERROR (
          @ERROR_MESSAGE,
          @ERROR_SEVERITY,
          @ERROR_STATE
          );
      END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW PAD IF NEEDED  
    --**************************************************************
      IF (@PAD_SCTRL_ID IS NULL) AND (@PAD_NAME IS NOT NULL)
        BEGIN TRY
          BEGIN TRAN

          SET @NEW_PAD_ORG_ID = (SELECT MAX(ORG_ID) + 1 FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2);

          --***** CREATE A NEW PAD / ORG
          INSERT INTO PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2
          (
            ACTIVE_IND 
            ,OPER_BUS_SEG_CD 
            ,ORG_NAME 
            ,ORG_CD 
            ,ID_ORG_TYPE
            ,CREATE_USER_ID
            ,CREATE_DT
            ,[USER_ID]
            ,UPDT_DT
            ,ORG_ID
          )
          VALUES
          (
            1
            ,'PAR'
            ,@PAD_NAME
            ,(@NEW_PAD_CODE + 'PAD')
            ,@PAD_ORG_TYPE  --THIS WAS THE VALUE GIVEN BY MELANIE on 3/28/19
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
            ,@NEW_PAD_ORG_ID
          );

          --***** ADD THE PAD TO THE HIERARCHY
          INSERT INTO PAR_UPS16_ESUITE.dbo.SCTRL_ORG_HIERARCHY_V2
          (  
            OPER_BUS_SEG_CD 
            ,ID_ORG 
            ,ID_ORG_PROFILE 
            ,ID_PARENTORG
            ,NOTES
            ,CREATE_USER_ID
            ,CREATE_DT
            ,[USER_ID]
            ,UPDT_DT
          )
          VALUES
          (
            'PAR'
            ,(SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_NAME = @PAD_NAME AND ID_ORG_TYPE = @PAD_ORG_TYPE)
            ,ISNULL((SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_PROFILE_V2 WHERE PROFILE_CD = 'PAD'), 97)
            ,NULL  --SET ID_PARENTORG = NULL PER MELANIE'S REQUEST ON 4/8/19
            ,NULL  --SET NOTES = NULL PER MELANIE'S REQUEST ON 4/8/19
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

          --***** LOG SUCCESS ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
          VALUES(1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum SCTRL_ORG_V2 Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI);
        COMMIT TRAN

        --***** PAD WAS CREATED *****
        SET @PAD_SCTRL_ID = (SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_V2 WHERE ORG_NAME = @PAD_NAME AND ID_ORG_TYPE = @PAD_ORG_TYPE);
      END TRY
      BEGIN CATCH
        ROLLBACK TRAN
          
        --***** PAD WAS NOT CREATED *****
        SET @PAD_SCTRL_ID = NULL;

        --***** LOG FAILURE ***** 
        INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
        VALUES(1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum SCTRL_ORG_V2 Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI);

        SELECT
          @ERROR_MESSAGE = ERROR_MESSAGE(),
          @ERROR_SEVERITY = ERROR_SEVERITY(),
          @ERROR_STATE = ERROR_STATE();

        RAISERROR (
          @ERROR_MESSAGE,
          @ERROR_SEVERITY,
          @ERROR_STATE
          );
      END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW GONL_PROP IF NEEDED  
    --**************************************************************
      IF (@EXISTING_WELL_ID IS NULL) AND (@NEW_WELL_ID IS NOT NULL)
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE WELL
          INSERT INTO [PAR_UPS16_QRA].[dbo].GONL_PROP(

            --***** IDENTIFIERS *****
            /*1*/  PROP_NO,    --PROPERTY_NUMBER
            /*2*/  ID_COST_CNTR,--COST CENTER
            /*3*/  BUS_UNIT_CD, --OPERATOR

            --***** MDM ATTRIBUTES *****
            /*4*/  PROP_NM,    --WELL_NAME
            /*5*/  ST_CD,    --STATE
            /*6*/  CNTY_CD,    --COUNTY
            /*7*/  API_ST_CD,  --STATE

            --***** SYSTEM ATTRIBUTES *****
            /*8*/  OPER_BUS_SEG_CD,
            /*9*/  CTRY_CD,
            /*10*/ PROP_OPER_FL,
            /*11*/ PROP_STAT_CD,
            /*12*/ PROP_TYPE_CD,
            /*13*/ LOC_CD,  
            /*14*/ UPDT_USER,
            /*15*/ UPDT_DT,
            /*16*/ APRV_FL,
            /*17*/ SRC_PROP_NO
          ) 
          VALUES
          (  

            --***** IDENTIFIERS *****
            /*1*/  @NEW_WELL_ID,
            /*2*/  (SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_COST_CNTR WHERE OPER_BUS_SEG_CD = 'PAR' AND COST_CNTR_CD = @PROPERTY_NUMBER),
            /*3*/  @EXISTING_OPERATOR_ID,

            --***** MDM ATTRIBUTES *****
            /*4*/  @WELL_NAME,
            /*5*/  @EXISTING_STATE_ID,
            /*6*/  @EXISTING_COUNTY_ID,
            /*7*/  @EXISTING_STATE_ID,

            --***** SYSTEM ATTRIBUTES *****
            /*8*/  'PAR',
            /*9*/  'US',
            /*10*/  'Y',
            /*11*/ 'A',
            /*12*/ 'WL',
            /*13*/ '1',  
            /*14*/ 'EH_WELL_WB_SP',
            /*15*/ DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*16*/ 'Y',
            /*17*/ @PROPERTY_NUMBER
          );

          --*****CREATE THE UWI
          INSERT INTO [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR (

            --***** IDENTIFIERS *****
            /*1*/  PROP_NO,

            --***** MDM ATTRIBUTES *****
            /*2*/  ATTR_VALUE,

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  OPER_BUS_SEG_CD,
            /*4*/  ATTR_TYPE_CD,
            /*5*/  UPDT_USER,
            /*6*/  UPDT_DT
          ) 
          VALUES
          (    
            --***** IDENTIFIERS *****
            /*1*/  @NEW_WELL_ID,

            --***** MDM ATTRIBUTES *****
            /*2*/  @UWI,

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  'PAR',
            /*4*/  'WI',
            /*5*/  'EH_WELL_WB_SP',
            /*6*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
            VALUES(1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI);
          COMMIT TRAN

          --***** WELL WAS CREATED *****
          SET @EXISTING_WELL_ID = @NEW_WELL_ID;
          SET @SOURCE_REFERENCE_NUM = @NEW_WELL_ID;
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** WELL WAS NOT CREATED *****
          SET @EXISTING_WELL_ID = NULL;
          SET @GONL_PROP_UWI = (SELECT ATTR_VALUE FROM [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR WHERE PROP_NO = @PROPERTY_NUMBER AND ATTR_TYPE_CD = 'WI' AND OPER_BUS_SEG_CD = 'PAR');
          SET @ERROR_MESSAGE = ERROR_MESSAGE();

          IF (@GONL_PROP_UWI <> @UWI) AND (@ERROR_MESSAGE LIKE '%Cannot insert duplicate key in object ''dbo.GONL_PROP''.%')
            BEGIN
              SET @ERROR_MESSAGE = 'UWI MISMATCH FOR WELL ' + ISNULL(@WELL_NAME, 'NULL') + ', PROP_NO: ' + ISNULL(@PROPERTY_NUMBER, 'NULL') + ' -> ENERHUB: ' + ISNULL(@UWI, 'NULL') + ', GONL_PROP_ATTR: ' + ISNULL(@GONL_PROP_UWI, 'NULL') 
            END

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
          VALUES(1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP Table Insert Error' + ': ' + @ERROR_MESSAGE, GetDate(), @WELL_LEVEL_TYPE, @UWI);

          SELECT
            --@ERROR_MESSAGE = ERROR_MESSAGE(),
            @ERROR_SEVERITY = ERROR_SEVERITY(),
            @ERROR_STATE = ERROR_STATE();

          RAISERROR (
            @ERROR_MESSAGE,
            @ERROR_SEVERITY,
            @ERROR_STATE
            );
        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW PAD ASSIGNMENT IF NEEDED  
    --**************************************************************
      IF (@EXISTING_WELL_PAD_ASSIGNMENT_ID IS NULL) AND (@PAD_SCTRL_ID IS NOT NULL)
        BEGIN TRY
          BEGIN TRAN

          --***** ASSIGN THE WELL TO THE PAD
          INSERT INTO PAR_UPS16_ESUITE.dbo.SCTRL_ORG_HIERARCHY_V2
          (  
            OPER_BUS_SEG_CD 
            ,ID_ORG 
            ,ID_ORG_PROFILE 
            ,ID_PARENTORG
            ,CREATE_USER_ID
            ,CREATE_DT
            ,[USER_ID]
            ,UPDT_DT
          )
          VALUES
          (
            'PAR'
            ,@EXISTING_WELL_SCTRL_ID
            ,ISNULL((SELECT TOP 1 ID FROM PAR_UPS16_ESUITE.dbo.SCTRL_ORG_PROFILE_V2 WHERE PROFILE_CD = 'PAD'), 97)
            ,@PAD_SCTRL_ID
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
            ,'EH_WELL_WB_SP'
            ,DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

          --***** LOG SUCCESS ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
          VALUES(1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum SCTRL_ORG_V2 Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI);
        COMMIT TRAN

        --***** PAD WAS CREATED *****
      END TRY
      BEGIN CATCH
        ROLLBACK TRAN
          
        --***** PAD WAS NOT CREATED *****
        SET @PAD_SCTRL_ID = NULL

        --***** LOG FAILURE ***** 
        INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
        VALUES(1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum SCTRL_ORG_V2 Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI);

        SELECT
          @ERROR_MESSAGE = ERROR_MESSAGE(),
          @ERROR_SEVERITY = ERROR_SEVERITY(),
          @ERROR_STATE = ERROR_STATE();

        RAISERROR (
          @ERROR_MESSAGE,
          @ERROR_SEVERITY,
          @ERROR_STATE
          );
      END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW BASIN IN GONL_PROP_ATTR IF NEEDED  
    --**************************************************************
      IF (@EXISTING_BASIN_ID IS NULL)
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE BASIN
          INSERT INTO [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR (

            --***** IDENTIFIERS *****
            /*1*/  PROP_NO,

            --***** MDM ATTRIBUTES *****
            /*2*/  ATTR_VALUE,

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  OPER_BUS_SEG_CD,
            /*4*/  ATTR_TYPE_CD,
            /*5*/  UPDT_USER,
            /*6*/  UPDT_DT
          ) 
          VALUES
          (    
            --***** IDENTIFIERS *****
            /*1*/  @EXISTING_WELL_ID,

            --***** MDM ATTRIBUTES *****
            /*2*/  @EXISTING_BASIN_XREF, --CHANGED TO XREF LOOKUP PER MELANIE'S REQUEST ON 4/3/19

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  'PAR',
            /*4*/  'BSN',
            /*5*/  'EH_WELL_WB_SP',
            /*6*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
            VALUES(1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI);
          COMMIT TRAN

        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** BASIN WAS NOT CREATED *****
          SET @EXISTING_BASIN_ID = NULL;

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
          VALUES(1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI);

          SELECT
            @ERROR_MESSAGE = ERROR_MESSAGE(),
            @ERROR_SEVERITY = ERROR_SEVERITY(),
            @ERROR_STATE = ERROR_STATE();

          RAISERROR (
            @ERROR_MESSAGE,
            @ERROR_SEVERITY,
            @ERROR_STATE
            );
        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW ORIENTATION IN GONL_PROP_ATTR IF NEEDED  
    --**************************************************************
      IF (@EXISTING_ORIENTATION_ID IS NULL)
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE ORIENTATION
          INSERT INTO [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR (

            --***** IDENTIFIERS *****
            /*1*/  PROP_NO,

            --***** MDM ATTRIBUTES *****
            /*2*/  ATTR_VALUE,

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  OPER_BUS_SEG_CD,
            /*4*/  ATTR_TYPE_CD,
            /*5*/  UPDT_USER,
            /*6*/  UPDT_DT
          ) 
          VALUES
          (    
            --***** IDENTIFIERS *****
            /*1*/  @EXISTING_WELL_ID,

            --***** MDM ATTRIBUTES *****
            /*2*/  @EXISTING_ORIENTATION_XREF, --CHANGED TO XREF LOOKUP PER MELANIE'S REQUEST ON 4/3/19

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  'PAR',
            /*4*/  'WOR',
            /*5*/  'EH_WELL_WB_SP',
            /*6*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** WELL WAS CREATED *****
          SET @EXISTING_WELL_ID = @NEW_WELL_ID;
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** WELL WAS NOT CREATED *****
          SET @EXISTING_WELL_ID = NULL;

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
          VALUES(1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI);

          SELECT
            @ERROR_MESSAGE = ERROR_MESSAGE(),
            @ERROR_SEVERITY = ERROR_SEVERITY(),
            @ERROR_STATE = ERROR_STATE();

          RAISERROR (
            @ERROR_MESSAGE,
            @ERROR_SEVERITY,
            @ERROR_STATE
            );
        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW ESI_ID IN GONL_PROP_ATTR IF NEEDED  
    --**************************************************************
      IF (@EXISTING_ESI_ID IS NULL) AND (@ESI_ID IS NOT NULL)
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE ESI_ID
          INSERT INTO [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR (

            --***** IDENTIFIERS *****
            /*1*/  PROP_NO,

            --***** MDM ATTRIBUTES *****
            /*2*/  ATTR_VALUE,

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  OPER_BUS_SEG_CD,
            /*4*/  ATTR_TYPE_CD,
            /*5*/  UPDT_USER,
            /*6*/  UPDT_DT
          ) 
          VALUES
          (    
            --***** IDENTIFIERS *****
            /*1*/  @EXISTING_WELL_ID,

            --***** MDM ATTRIBUTES *****
            /*2*/  @ESI_ID, 

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  'PAR',
            /*4*/  'ESI',
            /*5*/  'EH_WELL_WB_SP',
            /*6*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
            VALUES(1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Insert Successful ESI_ID', GetDate(), @WELL_LEVEL_TYPE, @UWI);
          COMMIT TRAN

          --***** ESI_ID WAS CREATED *****
          SET @EXISTING_ESI_ID = @ESI_ID;
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** ESI_ID WAS NOT CREATED *****
          SET @EXISTING_ESI_ID = NULL;

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]([CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI])
          VALUES(1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Insert Error ESI_ID' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI);

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(),
            @ERROR_SEVERITY = ERROR_SEVERITY(),
            @ERROR_STATE = ERROR_STATE();

          RAISERROR (@ERROR_MESSAGE,
              @ERROR_SEVERITY,
              @ERROR_STATE
              );
        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW FOREMAN IN GONL_PROP_ATTR IF NEEDED  
    --**************************************************************
      IF (@EXISTING_FOREMAN_ID IS NULL) AND (@FOREMAN_NAME IS NOT NULL)
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE FOREMAN
          INSERT INTO [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR (

            --***** IDENTIFIERS *****
            /*1*/  PROP_NO,

            --***** MDM ATTRIBUTES *****
            /*2*/  ATTR_VALUE,

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  OPER_BUS_SEG_CD,
            /*4*/  ATTR_TYPE_CD,
            /*5*/  UPDT_USER,
            /*6*/  UPDT_DT
          ) 
          VALUES
          (    
            --***** IDENTIFIERS *****
            /*1*/  @EXISTING_WELL_ID,

            --***** MDM ATTRIBUTES *****
            /*2*/  @FOREMAN_NAME, 

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  'PAR',
            /*4*/  'FOR',
            /*5*/  'EH_WELL_WB_SP',
            /*6*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Insert Successful Foreman', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** FOREMAN WAS CREATED *****
          SET @EXISTING_FOREMAN_ID = @FOREMAN_NAME;
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** FOREMAN WAS NOT CREATED *****
          SET @EXISTING_FOREMAN_ID = NULL;

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Insert Error Foreman' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          );

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(),
            @ERROR_SEVERITY = ERROR_SEVERITY(),
            @ERROR_STATE = ERROR_STATE();

          RAISERROR (@ERROR_MESSAGE,
              @ERROR_SEVERITY,
              @ERROR_STATE
              );
        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW PUMPER IN GONL_PROP_ATTR IF NEEDED  
    --**************************************************************
      IF (@EXISTING_PUMPER_ID IS NULL) AND (@PUMPER_NAME IS NOT NULL)
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE PUMPER
          INSERT INTO [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR (

            --***** IDENTIFIERS *****
            /*1*/  PROP_NO,

            --***** MDM ATTRIBUTES *****
            /*2*/  ATTR_VALUE,

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  OPER_BUS_SEG_CD,
            /*4*/  ATTR_TYPE_CD,
            /*5*/  UPDT_USER,
            /*6*/  UPDT_DT
          ) 
          VALUES
          (    
            --***** IDENTIFIERS *****
            /*1*/  @EXISTING_WELL_ID,

            --***** MDM ATTRIBUTES *****
            /*2*/  @PUMPER_NAME, 

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  'PAR',
            /*4*/  'PUM',
            /*5*/  'EH_WELL_WB_SP',
            /*6*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Insert Successful Pumper', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** PUMPER WAS CREATED *****
          SET @EXISTING_PUMPER_ID = @PUMPER_NAME;
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** PUMPER WAS NOT CREATED *****
          SET @EXISTING_PUMPER_ID = NULL;

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Insert Error Pumper' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          );

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(),
            @ERROR_SEVERITY = ERROR_SEVERITY(),
            @ERROR_STATE = ERROR_STATE();

          RAISERROR (@ERROR_MESSAGE,
              @ERROR_SEVERITY,
              @ERROR_STATE
              );
        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW PONL_WELL IF NEEDED  
    --**************************************************************
      IF (@EXISTING_PONL_WELL_ID IS NULL)
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE PONL_WELL
          INSERT INTO [PAR_UPS16_QRA].[dbo].PONL_WELL(

            --***** IDENTIFIERS *****
            /*1*/  WELL_NO,      --PROPERTY_NUMBER
            /*2*/  BUS_UNIT_CD,    --OPERATOR

            --***** MDM ATTRIBUTES *****
            /*3*/  API_WELL_NO,    --WELL_GOVERNMENT_ID

            --***** SYSTEM ATTRIBUTES *****
            /*4*/  OPER_BUS_SEG_CD,
            /*5*/  OWNRSHIP_TYPE_CD,
            /*6*/  WELL_NM,
            /*7*/  COMPL_CNT,   
            /*8*/  CREATE_DT,
            /*9*/  UPDT_USER,
            /*10*/ UPDT_DT
          ) 
          VALUES
          (  

            --***** IDENTIFIERS *****
            /*1*/  @EXISTING_WELL_ID,
            /*2*/  @EXISTING_OPERATOR_ID,

            --***** MDM ATTRIBUTES *****
            /*3*/  NULLIF(LEFT(@WELL_GOVERNMENT_ID, 2) + '-' + 
                    SUBSTRING(REPLACE(@WELL_GOVERNMENT_ID, '-', ''), 3, 3) + '-' + 
                    SUBSTRING(REPLACE(@WELL_GOVERNMENT_ID, '-', ''), 6, 5), '--'),    --***** ENERHUB DOES NOT SEND DASHES SO WE NEED TO ADD THEM OR QUORUM WILL NOT WORK PROPERLY (IT EXPECTS FORMAT AS ##-###-#####)

            --***** SYSTEM ATTRIBUTES *****
            /*4*/  'PAR',
            /*5*/  1,
            /*6*/  @WELL_NAME,
            /*7*/  1,    
            /*8*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*9*/  'EH_WELL_WB_SP',
            /*10*/ DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** PONL_WELL WAS CREATED *****
          SET @EXISTING_PONL_WELL_ID = @EXISTING_WELL_ID
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** PONL_WELL WAS NOT CREATED *****
          SET @EXISTING_PONL_WELL_ID = NULL

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          );

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(),
            @ERROR_SEVERITY = ERROR_SEVERITY(),
            @ERROR_STATE = ERROR_STATE();

          RAISERROR (@ERROR_MESSAGE,
              @ERROR_SEVERITY,
              @ERROR_STATE
              );
        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW GONL_CMNT_HDR IF NEEDED  
    --**************************************************************
      IF (@EXISTING_CMNT_HEADER_ID IS NULL) AND (@WELL_NAME_FKA IS NOT NULL)  --ONLY CREATE THIS RECORD IF AN FKA NAME EXISTS
        BEGIN TRY
          BEGIN TRAN
		    -- Generate the New Sequence number using the storedprocedure to update the seq number in the QARCH_TRAN_SEQ to prevent multiple records having the same sequence number
	       EXEC [PAR_UPS16_QRA].[dbo].QARCH_GETNEXTSEQ 'CMNT_SEQ_NO',1,@NEW_CMNT_HEADER_ID Output 

          --*****CREATE THE GONL_CMNT_DETAIL
          INSERT INTO [PAR_UPS16_QRA].[dbo].GONL_CMNT_HDR(

            --***** IDENTIFIERS *****
            /*1*/  CMNT_SEQ_NO,      

            --***** MDM ATTRIBUTES *****
            

            --***** SYSTEM ATTRIBUTES *****
            /*2*/  CMNT_GRP_CD,
            /*3*/  CMNT_TBL_NM_ID,
            /*4*/  UPDT_USER,
            /*5*/  UPDT_DT
          ) 
          VALUES
          (  

            --***** IDENTIFIERS *****
            /*1*/  @NEW_CMNT_HEADER_ID,

            --***** MDM ATTRIBUTES *****


            --***** SYSTEM ATTRIBUTES *****
            /*2*/  'PR',
            /*3*/  'GONL_PROP',
            /*4*/  'EH_WELL_WB_SP',
            /*5*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_CMNT_HDR Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** GONL_CMNT_DETAIL WAS CREATED *****
          SET @EXISTING_CMNT_HEADER_ID = @NEW_CMNT_HEADER_ID
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** GONL_CMNT_HDR WAS NOT CREATED *****
          SET @EXISTING_CMNT_HEADER_ID = NULL;
          SET @NEW_CMNT_HEADER_ID = NULL;

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_CMNT_HDR Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          );

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(),
            @ERROR_SEVERITY = ERROR_SEVERITY(),
            @ERROR_STATE = ERROR_STATE();

          RAISERROR (@ERROR_MESSAGE,
              @ERROR_SEVERITY,
              @ERROR_STATE
              );
        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW GONL_CMNT_DETAIL IF NEEDED  
    --**************************************************************
      IF (@EXISTING_CMNT_DETAIL_ID IS NULL) AND (@WELL_NAME_FKA IS NOT NULL)  --ONLY CREATE THIS RECORD IF AN FKA NAME EXISTS
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE GONL_CMNT_DETAIL
          INSERT INTO [PAR_UPS16_QRA].[dbo].GONL_CMNT_DETAIL(

            --***** IDENTIFIERS *****
            /*1*/  CMNT_SEQ_NO,      

            --***** MDM ATTRIBUTES *****
            /*2*/  CMNT,    --WELL_NAME_FKA

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  CMNT_TYPE_CD,
            /*4*/  CMNT_GRP_CD,
            /*5*/  CMNT_TBL_NM_ID,
            /*6*/  CMNT_CREATE_DT,
            /*7*/  UPDT_USER,
            /*8*/  UPDT_DT
          ) 
          VALUES
          (  

            --***** IDENTIFIERS *****
            /*1*/  @EXISTING_CMNT_HEADER_ID,

            --***** MDM ATTRIBUTES *****
            /*2*/  @WELL_NAME_FKA,

            --***** SYSTEM ATTRIBUTES *****
            /*3*/  'FKA',
            /*4*/  'PR',
            /*5*/  'GONL_PROP',
            /*6*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*7*/  'EH_WELL_WB_SP',
            /*8*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_CMNT_DETAIL Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** GONL_CMNT_DETAIL WAS CREATED *****
          SET @EXISTING_CMNT_DETAIL_ID = @EXISTING_CMNT_HEADER_ID;
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** GONL_CMNT_DETAIL WAS NOT CREATED *****
          SET @EXISTING_CMNT_DETAIL_ID = NULL;

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_CMNT_DETAIL Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          );

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW GONL_PROP_EFF_DT IF NEEDED  
    --**************************************************************
      IF (@EXISTING_GONL_PROP_EFF_ID IS NULL)
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE TONL_TX_CPA_MST
          INSERT INTO [PAR_UPS16_QRA].[dbo].GONL_PROP_EFF_DT(

            --***** IDENTIFIERS *****
            /*1*/  PROP_NO,  

            --***** MDM ATTRIBUTES *****


            --***** SYSTEM ATTRIBUTES *****
            /*2*/  OPER_BUS_SEG_CD,
            /*3*/  EFF_DT_FROM,
            /*4*/  EFF_DT_TO,
            /*5*/  BA_NO,
            /*6*/  BA_SUB,
            /*7*/  UPDT_USER,
            /*8*/  UPDT_DT
          ) 
          VALUES
          (  
            --***** IDENTIFIERS *****
            /*1*/  @EXISTING_WELL_ID,

            --***** MDM ATTRIBUTES *****

            --***** SYSTEM ATTRIBUTES *****
            /*2*/  'PAR',
            /*3*/  @CURRENT_MONTH_START,
            /*4*/  '9999-12-31 00:00:00.000',
            /*5*/  '001',  
            /*6*/  1,
            /*7*/  'EH_WELL_WB_SP',
            /*8*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_EFF_DT Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** GONL_PROP_EFF_DT WAS CREATED *****
          SET @EXISTING_GONL_PROP_EFF_ID = @EXISTING_WELL_ID
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** PONL_WELL_COMPL_EFF_DT WAS NOT CREATED *****
          SET @EXISTING_GONL_PROP_EFF_ID = NULL

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_EFF_DT Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          );

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW WELL STATUS/WELL CLASS IN PONL_WELL_COMPL_EFF_DT IF NEEDED  
    --**************************************************************
      IF (@EXISTING_PONL_WELL_COMPL_EFF_ID IS NULL)
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE PONL_WELL_COMPL_EFF_DT
          INSERT INTO [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT(

            --***** IDENTIFIERS *****
            /*1*/  WELL_NO,  
            /*2*/  COMPL_NO,  

            --***** MDM ATTRIBUTES *****
            /*3*/  WELL_CLASS_CD,    --WELL_CLASS_NAME
            /*4*/  WELL_STAT_CD,    --WELL_STATUS

            --***** SYSTEM ATTRIBUTES *****
            /*5*/  OPER_BUS_SEG_CD,
            /*6*/  EFF_DT_FROM,
            /*7*/  EFF_DT_TO,
            /*8*/  OPER_FL,
            /*9*/  UPDT_USER,
            /*10*/ UPDT_DT,
            /*11*/ EXP_FL
          ) 
          VALUES
          (  
            --***** IDENTIFIERS *****
            /*1*/  @EXISTING_WELL_ID,
            /*2*/  1,

            --***** MDM ATTRIBUTES *****
            /*3*/  @WELL_CLASS_NAME,
            /*4*/   ISNULL(@NEW_WELL_STATUS_ID, '2020'),  --WELL CLASS CHANGED, USE DRILLING-EFFECTIVE DATES FROM PARAMETER SECTION ABOVE

            --***** SYSTEM ATTRIBUTES *****
            /*5*/  'PAR',
            /*6*/  @WELL_STATUS_EFFECTIVE_DATE, --CAST(DATEADD(MILLISECOND, -DATEPART(MILLISECOND, @STATUS_NEW_START_DATE), @STATUS_NEW_START_DATE) AS DATE),
            /*7*/  '9999-12-31 00:00:00.000',
            /*8*/  'Y',  
            /*9*/  'EH_WELL_WB_SP',
            /*10*/ DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*11*/ 'N'
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL_COMPL_EFF_DT Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** PONL_WELL_COMPL_EFF_DT WAS CREATED *****
          SET @EXISTING_PONL_WELL_COMPL_EFF_ID = @EXISTING_WELL_ID
          SET @EXISTING_WELL_STATUS_ID = @NEW_WELL_STATUS_ID
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** PONL_WELL_COMPL_EFF_DT WAS NOT CREATED *****
          SET @EXISTING_PONL_WELL_COMPL_EFF_ID = NULL
          SET @EXISTING_WELL_STATUS_ID = NULL
          SET @WELL_STATUS_CHANGED = 0
		  SET @ERROR_STATUS_DUPLICATE = 1
          SET @ERROR_MESSAGE = ERROR_MESSAGE();

          IF @ERROR_MESSAGE LIKE '%Cannot insert duplicate key in object ''dbo.PONL_WELL_COMPL_EFF_DT''.%'
            BEGIN
              SET @ERROR_MESSAGE = 'DUPLICATE STATUS DATES: THIS WELL ALREADY HAS A STATUS WITH AN EFF_DT_FROM OF [' + ISNULL(CONVERT(VARCHAR(50), @WELL_STATUS_EFFECTIVE_DATE), 'NULL') + '], UNABLE TO INSERT NEW STATUS WITH THE EXACT SAME DATETIME.'
              SET @ERROR_SEVERITY = 16;
              SET @ERROR_STATE = 1;
            END

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL_COMPL_EFF_DT Table Insert Error' + ': ' + @ERROR_MESSAGE, GetDate(), @WELL_LEVEL_TYPE, @UWI
          );

          SELECT @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --CREATE NEW TONL_TX_CPA_MST IF NEEDED  
    --**************************************************************
      IF (@EXISTING_TONL_TX_ID IS NULL) --AND (@RRC_LEASE_ID IS NOT NULL)
        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE TONL_TX_CPA_MST
          INSERT INTO [PAR_UPS16_QRA].[dbo].TONL_TX_CPA_MST(

            --***** IDENTIFIERS *****
            --/*1*/  TX_CPA_SEQ_NO,  
            /*2*/  WELL_NO,  
            /*3*/  COMPL_NO,

            --***** MDM ATTRIBUTES *****
            /*4*/  DRL_PRMT_NO,    --DRILLING_PERMIT_NUMBER
            /*5*/  LSE_NO,      --RRC_LEASE_ID
            /*6*/  DRILL_PRMT_FL,   --REQUESTED BY MELANIE ON 3/29/19

            --***** SYSTEM ATTRIBUTES *****
            /*7*/  OPER_BUS_SEG_CD,
            /*8*/  EFF_DT_FROM,
            /*9*/  EFF_DT_TO,
            /*10*/ LSE_NM,   
            /*11*/ TAXPR_NO,
            /*12*/ BUS_UNIT_CD,        
            /*13*/ CTRY_CD,
            /*14*/ ST_CD,
            /*15*/ UPDT_USER,
            /*16*/ UPDT_DT,
            /*17*/ PURCH_TAXPR_NO
          ) 
          VALUES
          (  

            --***** IDENTIFIERS *****
            --/*1*/  (SELECT MAX(TX_CPA_SEQ_NO) + 1 FROM [PAR_UPS16_QRA].[dbo].[TONL_TX_CPA_MST]),
            /*2*/  @EXISTING_WELL_ID,
            /*3*/  1,

            --***** MDM ATTRIBUTES *****
            /*4*/  @DRILLING_PERMIT_NUMBER,
            /*5*/  CASE WHEN @RRC_LEASE_ID IS NOT NULL AND @RRC_LEASE_ID <> '' THEN @RRC_LEASE_ID 
						WHEN (@RRC_LEASE_ID IS NULL OR @RRC_LEASE_ID = '') AND @DRILLING_PERMIT_NUMBER IS NOT NULL THEN @DRILLING_PERMIT_NUMBER
						ELSE '' END,  
            /*6*/  CASE WHEN @RRC_LEASE_ID IS NOT NULL AND @RRC_LEASE_ID <> '' THEN 'N'
						WHEN (@RRC_LEASE_ID IS NULL OR @RRC_LEASE_ID = '') AND @DRILLING_PERMIT_NUMBER IS NOT NULL THEN 'Y'
						WHEN @DRILLING_PERMIT_NUMBER IS NOT NULL THEN 'Y' 
                  ELSE 'N' END,

            --***** SYSTEM ATTRIBUTES *****
            /*7*/  'PAR',
            /*8*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, @CURRENT_MONTH_START), @CURRENT_MONTH_START),
            /*9*/  '9999-12-31 00:00:00.000',
            /*10*/  @REGULATORY_LEASE_NAME,
            /*11*/ '32034745193',
            /*12*/ @EXISTING_OPERATOR_ID,
            /*13*/ 'US',
            /*14*/ @EXISTING_STATE_ID,
            /*15*/ 'EH_WELL_WB_SP',
            /*16*/ DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*17*/ '999999999'
          );

          --*****INSERT A COMPANION RECORD INTO GXRF_DESKID_TAX PER MELANIE's REQUEST ON 3/29/19
          IF @EXISTING_TAX_DESK_ID IS NULL
            BEGIN
              INSERT INTO [PAR_UPS16_QRA].[dbo].GXRF_DESKID_TAX(

                --***** IDENTIFIERS *****
                /*1*/  WELL_NO,  
                /*2*/  COMPL_NO,

                --***** MDM ATTRIBUTES *****

                --***** SYSTEM ATTRIBUTES *****
                /*3*/  OPER_BUS_SEG_CD,
                /*4*/  DESKID_NO,
                /*5*/  UPDT_USER,
                /*6*/  UPDT_DT
              ) 
              VALUES
              (  

                --***** IDENTIFIERS *****
                /*1*/  @EXISTING_WELL_ID,
                /*2*/  1,

                --***** MDM ATTRIBUTES *****

                --***** SYSTEM ATTRIBUTES *****
                /*3*/  'PAR',
                /*4*/  'ENERHUB',
                /*5*/  'EH_WELL_WB_SP',
                /*6*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE())
              );
            END

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum TONL_TX_CPA_MST Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** TONL_TX_CPA_MST WAS CREATED *****
          SET @EXISTING_TONL_TX_ID = (SELECT TOP 1 TX_CPA_SEQ_NO FROM [PAR_UPS16_QRA].[dbo].TONL_TX_CPA_MST WHERE OPER_BUS_SEG_CD = 'PAR' AND EFF_DT_TO > GETDATE() AND WELL_NO = @EXISTING_WELL_ID AND EFF_DT_FROM = (SELECT MAX(EFF_DT_FROM) FROM [PAR_UPS16_QRA].[dbo].TONL_TX_CPA_MST WHERE OPER_BUS_SEG_CD = 'PAR' AND EFF_DT_TO > GETDATE() AND WELL_NO = @EXISTING_WELL_ID));
          SET @EXISTING_PONL_WELL_ID = @EXISTING_WELL_ID
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** TONL_TX_CPA_MST WAS NOT CREATED *****
          SET @EXISTING_PONL_WELL_ID = NULL

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum TONL_TX_CPA_MST Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          );

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END
    
    BEGIN  --***** UPDATE BATTERY ASSIGNMENT IF NEEDED  
    --**************************************************************
      IF (@EXISTING_WELL_BATTERY_ASSIGNMENT_ID IS NOT NULL) AND (@BATTERY_SCTRL_ID IS NOT NULL)  
        BEGIN TRY 
          BEGIN TRAN   
            UPDATE [PAR_UPS16_QRA].[dbo].SCTRL_ORG_HIERARCHY_V2 SET 
              /*1*/  ID_PARENTORG          = @BATTERY_SCTRL_ID,    
              /*2*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
              /*3*/  [USER_ID]          = 'EH_WELL_WB_SP' 
            WHERE ID = @EXISTING_WELL_BATTERY_ASSIGNMENT_ID AND
                OPER_BUS_SEG_CD = 'PAR' AND 
                ID_ORG_PROFILE = @ORG_PROFILE_ID;

            --***** LOG SUCCESS *****  
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum SCTRL_ORG_HIERARCHY_V2 Table Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            )
          COMMIT TRAN

          --***** BATTERY ASSIGNMENT WAS UPDATED *****
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** BATTERY ASSIGNMENT WAS NOT UPDATED *****
          SET @EXISTING_BATTERY_ORG_ID = NULL;

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum SCTRL_ORG_HIERARCHY_V2 Table Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          )

            SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
            RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END
    
    BEGIN  --***** UPDATE PAD ASSIGNMENT IF NEEDED  
    --**************************************************************
      IF (@EXISTING_WELL_PAD_ASSIGNMENT_ID IS NOT NULL) AND (@PAD_SCTRL_ID IS NOT NULL) 
        BEGIN TRY 
          BEGIN TRAN   
            UPDATE [PAR_UPS16_QRA].[dbo].SCTRL_ORG_HIERARCHY_V2 SET 
              /*1*/  ID_PARENTORG          = @PAD_SCTRL_ID,  
              /*2*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
              /*3*/  [USER_ID]          = 'EH_WELL_WB_SP' 
            WHERE ID = @EXISTING_WELL_PAD_ASSIGNMENT_ID AND
                OPER_BUS_SEG_CD = 'PAR';

            --***** LOG SUCCESS *****  
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum SCTRL_ORG_HIERARCHY_V2 Table Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            )
          COMMIT TRAN

          --***** PAD ASSIGNMENT WAS UPDATED *****
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** PAD ASSIGNMENT WAS NOT UPDATED *****
          SET @PAD_SCTRL_ID = NULL;

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum SCTRL_ORG_HIERARCHY_V2 Table Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          )

            SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
            RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE ATTRIBUTES IN GONL_PROP  
    --**************************************************************
      BEGIN TRY 
        BEGIN TRAN   
          UPDATE [PAR_UPS16_QRA].[dbo].GONL_PROP SET 
            /*1*/  PROP_NM            = @WELL_NAME,            
            /*2*/  ST_CD            = ISNULL(@EXISTING_STATE_ID, ST_CD),
            /*3*/  CNTY_CD            = ISNULL(@EXISTING_COUNTY_ID, '000'), 
            /*4*/  API_ST_CD          = ISNULL(@EXISTING_STATE_ID, API_ST_CD),
            /*5*/  ID_COST_CNTR          = ISNULL(@EXISTING_COST_CENTER_ID, ID_COST_CNTR),
            /*6*/  CMNT_SEQ_NO          = ISNULL(@EXISTING_CMNT_DETAIL_ID, CMNT_SEQ_NO),
            /*7*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*8*/  UPDT_USER          = 'EH_WELL_WB_SP' 
          WHERE PROP_NO = @EXISTING_WELL_ID AND
              OPER_BUS_SEG_CD = 'PAR'

          --***** LOG SUCCESS *****  
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP Table Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
          )
        COMMIT TRAN

        --***** WELL WAS UPDATED *****

      END TRY
      BEGIN CATCH
        ROLLBACK TRAN
          
        --***** WELL WAS NOT UPDATED *****

        --***** LOG FAILURE ***** 
        INSERT INTO [Enerhub].[CallOutAuditLog]
        (
          [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
        )
        VALUES
        (
          1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP Table Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
        )

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

      END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE WELL_NAME_FKA IN GONL_CMNT_DETAIL    
    --**************************************************************
      IF (@EXISTING_CMNT_DETAIL_ID IS NOT NULL) AND (@WELL_NAME_FKA IS NOT NULL)  --ONLY CREATE THIS RECORD IF AN FKA NAME EXISTS
        BEGIN TRY 
          BEGIN TRAN   
            UPDATE [PAR_UPS16_QRA].[dbo].GONL_CMNT_DETAIL SET 
              /*1*/  CMNT              = ISNULL(@WELL_NAME_FKA, CMNT),            
              /*2*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
              /*3*/  UPDT_USER          = 'EH_WELL_WB_SP' 
            WHERE CMNT_SEQ_NO = @EXISTING_CMNT_HEADER_ID AND
                CMNT_TYPE_CD = 'FKA' AND
                CMNT_GRP_CD = 'PR' AND
                CMNT_TBL_NM_ID = 'GONL_PROP'

            --***** LOG SUCCESS *****  
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_CMNT_DETAIL Table Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            )
          COMMIT TRAN

          --***** COMMENT WAS UPDATED *****

        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** COMMENT WAS NOT UPDATED *****

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_CMNT_DETAIL Table Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          )

            SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
            RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE UWI IN GONL_PROP_ATTR  
    --**************************************************************
      BEGIN TRY 
        BEGIN TRAN   
          UPDATE [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR SET 
            /*1*/  ATTR_VALUE          = @UWI,            
            /*2*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*3*/  UPDT_USER          = 'EH_WELL_WB_SP' 
          WHERE PROP_NO = @EXISTING_WELL_ID AND
              OPER_BUS_SEG_CD = 'PAR' AND
              ATTR_TYPE_CD = 'WI'

          --***** LOG SUCCESS *****  
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
          )
        COMMIT TRAN

        --***** WELL WAS UPDATED *****

      END TRY
      BEGIN CATCH
        ROLLBACK TRAN
          
        --***** WELL WAS NOT UPDATED *****

        --***** LOG FAILURE ***** 
        INSERT INTO [Enerhub].[CallOutAuditLog]
        (
          [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
        )
        VALUES
        (
          1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
        )

        SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
        RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

      END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE BASIN IN GONL_PROP_ATTR  
    --**************************************************************
      BEGIN TRY 
        BEGIN TRAN   
          UPDATE [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR SET 
            /*1*/  ATTR_VALUE          = ISNULL(@EXISTING_BASIN_XREF, ISNULL(ATTR_VALUE, 'MIDLAND')),     --CHANGED TO XREF LOOKUP PER MELANIE'S REQUEST ON 4/3/19        
            /*2*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*3*/  UPDT_USER          = 'EH_WELL_WB_SP' 
          WHERE PROP_NO = @EXISTING_WELL_ID AND
              OPER_BUS_SEG_CD = 'PAR' AND
              ATTR_TYPE_CD = 'BSN'

          --***** LOG SUCCESS *****  
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
          )
        COMMIT TRAN

        --***** WELL WAS UPDATED *****

      END TRY
      BEGIN CATCH
        ROLLBACK TRAN
          
        --***** WELL WAS NOT UPDATED *****

        --***** LOG FAILURE ***** 
        INSERT INTO [Enerhub].[CallOutAuditLog]
        (
          [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
        )
        VALUES
        (
          1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
        )

        SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
        RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

      END CATCH
    --**************************************************************
    END
    
    BEGIN  --***** UPDATE DIVESTITURE IN GONL_PROP_ATTR  
    --**************************************************************
      BEGIN TRY 
        BEGIN TRAN   
          UPDATE [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR SET 
            /*1*/  ATTR_VALUE          = ISNULL(@EXISTING_DIVESTITURE_XREF, ATTR_VALUE),           
            /*2*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*3*/  UPDT_USER          = 'EH_WELL_WB_SP' 
          WHERE PROP_NO = @EXISTING_WELL_ID AND
              OPER_BUS_SEG_CD = 'PAR' AND
              ATTR_TYPE_CD = 'DIV'

          --***** LOG SUCCESS *****  
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
          )
        COMMIT TRAN

        --***** WELL WAS UPDATED *****

      END TRY
      BEGIN CATCH
        ROLLBACK TRAN
          
        --***** WELL WAS NOT UPDATED *****

        --***** LOG FAILURE ***** 
        INSERT INTO [Enerhub].[CallOutAuditLog]
        (
          [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
        )
        VALUES
        (
          1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
        )

        SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
        RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

      END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE ESI_ID IN GONL_PROP_ATTR  
    --**************************************************************
      IF @EXISTING_ESI_ID IS NOT NULL
        BEGIN TRY 
          BEGIN TRAN

            UPDATE [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR SET 
              /*1*/  ATTR_VALUE          = ISNULL(@ESI_ID, ATTR_VALUE),           
              /*2*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
              /*3*/  UPDT_USER          = 'EH_WELL_WB_SP' 
            WHERE PROP_NO = @EXISTING_WELL_ID AND
                OPER_BUS_SEG_CD = 'PAR' AND
                ATTR_TYPE_CD = 'ESI'

            --***** LOG SUCCESS *****  
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Successful ESI ID', GetDate(), @WELL_LEVEL_TYPE, @UWI
            )
          COMMIT TRAN

          --***** WELL WAS UPDATED *****

        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** WELL WAS NOT UPDATED *****

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Error ESI ID' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          )

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE FOREMAN IN GONL_PROP_ATTR  
    --**************************************************************
      IF @EXISTING_FOREMAN_ID IS NOT NULL
        BEGIN TRY 
          BEGIN TRAN   
            UPDATE [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR SET 
              /*1*/  ATTR_VALUE          = ISNULL(@FOREMAN_NAME, ATTR_VALUE),         
              /*2*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
              /*3*/  UPDT_USER          = 'EH_WELL_WB_SP' 
            WHERE PROP_NO = @EXISTING_WELL_ID AND
                OPER_BUS_SEG_CD = 'PAR' AND
                ATTR_TYPE_CD = 'FOR'

            --***** LOG SUCCESS *****  
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Successful Foreman', GetDate(), @WELL_LEVEL_TYPE, @UWI
            )
          COMMIT TRAN

          --***** WELL WAS UPDATED *****

        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** WELL WAS NOT UPDATED *****

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Error Foreman' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          )

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE PUMPER IN GONL_PROP_ATTR  
    --**************************************************************
      IF @EXISTING_PUMPER_ID IS NOT NULL
        BEGIN TRY 
          BEGIN TRAN   
            UPDATE [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR SET 
              /*1*/  ATTR_VALUE          = ISNULL(@PUMPER_NAME, ATTR_VALUE),            
              /*2*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
              /*3*/  UPDT_USER          = 'EH_WELL_WB_SP' 
            WHERE PROP_NO = @EXISTING_WELL_ID AND
                OPER_BUS_SEG_CD = 'PAR' AND
                ATTR_TYPE_CD = 'PUM'

            --***** LOG SUCCESS *****  
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Successful Pumper', GetDate(), @WELL_LEVEL_TYPE, @UWI
            )
          COMMIT TRAN

          --***** WELL WAS UPDATED *****

        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** WELL WAS NOT UPDATED *****

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Error Pumper' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          )

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE ORIENTATION IN GONL_PROP_ATTR  
    --**************************************************************
      BEGIN TRY 
        BEGIN TRAN   
          UPDATE [PAR_UPS16_QRA].[dbo].GONL_PROP_ATTR SET 
            /*1*/  ATTR_VALUE          = ISNULL(@EXISTING_ORIENTATION_XREF, ISNULL(ATTR_VALUE, 'HRZ')),     --CHANGED TO XREF LOOKUP PER MELANIE'S REQUEST ON 4/3/19        
            /*2*/  UPDT_DT            = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*3*/  UPDT_USER          = 'EH_WELL_WB_SP' 
          WHERE PROP_NO = @EXISTING_WELL_ID AND
              OPER_BUS_SEG_CD = 'PAR' AND
              ATTR_TYPE_CD = 'WOR'

          --***** LOG SUCCESS *****  
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Successful Orientation', GetDate(), @WELL_LEVEL_TYPE, @UWI
          )
        COMMIT TRAN

        --***** WELL WAS UPDATED *****

      END TRY
      BEGIN CATCH
        ROLLBACK TRAN
          
        --***** WELL WAS NOT UPDATED *****

        --***** LOG FAILURE ***** 
        INSERT INTO [Enerhub].[CallOutAuditLog]
        (
          [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
        )
        VALUES
        (
          1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum GONL_PROP_ATTR Table Update Error Orientation' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
        )

        SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
        RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

      END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE ATTRIBUTES IN PONL_WELL  
    --**************************************************************
      BEGIN TRY 
        BEGIN TRAN   
          UPDATE [PAR_UPS16_QRA].[dbo].PONL_WELL SET 
            /*1*/  API_WELL_NO            = ISNULL(NULLIF(LEFT(@WELL_GOVERNMENT_ID, 2) + '-' + 
                                        SUBSTRING(REPLACE(@WELL_GOVERNMENT_ID, '-', ''), 3, 3) + '-' + 
                                        SUBSTRING(REPLACE(@WELL_GOVERNMENT_ID, '-', ''), 6, 5), '--'), API_WELL_NO),    --***** ENERHUB DOES NOT SEND DASHES SO WE NEED TO ADD THEM OR QUORUM WILL NOT WORK PROPERLY (IT EXPECTS FORMAT AS ##-###-#####)
            /*2*/  WELL_NM              = @WELL_NAME,
            /*3*/  UPDT_DT              = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*4*/  UPDT_USER            = 'EH_WELL_WB_SP' 
          WHERE WELL_NO = @EXISTING_WELL_ID AND
              OPER_BUS_SEG_CD = 'PAR'

          --***** LOG SUCCESS *****  
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL Table Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
          )
        COMMIT TRAN

        --***** WELL WAS UPDATED *****

      END TRY
      BEGIN CATCH
        ROLLBACK TRAN
          
        --***** WELL WAS NOT UPDATED *****

        --***** LOG FAILURE ***** 
        INSERT INTO [Enerhub].[CallOutAuditLog]
        (
          [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
        )
        VALUES
        (
          1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL Table Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
        )

        SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
        RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

      END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE STATUS/WELL CLASS ATTRIBUTES IN PONL_WELL_COMPL_EFF_DT  
    --**************************************************************
      IF (@EXISTING_PONL_WELL_COMPL_EFF_ID IS NOT NULL)
        BEGIN TRY 
          BEGIN TRAN   
            UPDATE [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT SET 
              /*1*/  WELL_CLASS_CD            = CASE WHEN (DATEDIFF(DAY, @EXISTING_WELL_CLASS_EFF_DT_FROM, GETDATE()) = 0) THEN ISNULL(@WELL_CLASS_NAME, WELL_CLASS_CD)  --ONLY UPDATE THE WELL CLASS IF THE RECORD WAS ALSO EFFECTIVE DATED TODAY
                                        ELSE WELL_CLASS_CD END,    
              /*2*/  EFF_DT_TO              = CASE WHEN @WELL_CLASS_CHANGED = 1 AND (ISNULL(EFF_DT_FROM, @CURRENT_MONTH_START) < CAST(GETDATE() AS DATE))  THEN DATEADD(MILLISECOND, -DATEPART(MILLISECOND, @YESTERDAY), @YESTERDAY)  --IF THE WELL CLASS HAS CHANGED NOT ON THE SAME DAY THEN WE NEED TO SET THE END DATE = YESTERDAY FOR THE CURRENT RECORD AND ADD A NEW RECORD BELOW
                                        WHEN @WELL_STATUS_CHANGED = 1 THEN ISNULL(@WELL_STATUS_EFFICTIVE_DATE_TO, EFF_DT_TO) --IF THE WELL STATUS HAS CHANGED THEN WE NEED TO SET THE END DATE = STATUS END DATE FOR THE CURRENT RECORD AND ADD A NEW RECORD BELOW
                                        ELSE '9999-12-31 00:00:00.000' END,
              /*3*/  UPDT_DT                = DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
              /*4*/  UPDT_USER              = 'EH_WELL_WB_SP' 
            WHERE WELL_NO = @EXISTING_WELL_ID AND
                COMPL_NO = 1 AND
                OPER_BUS_SEG_CD = 'PAR' AND
                WELL_STAT_CD = @EXISTING_WELL_STATUS_ID AND 
                EFF_DT_FROM = @EXISTING_WELL_CLASS_EFF_DT_FROM;

            --***** LOG SUCCESS *****  
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL_COMPL_EFF_DT Table Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            )
          COMMIT TRAN

          --***** WELL WAS UPDATED *****

        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** WELL WAS NOT UPDATED *****

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL_COMPL_EFF_DT Table Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          )

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --***** UPDATE ATTRIBUTES IN TONL_TX_CPA_MST  
    --**************************************************************
      IF (@EXISTING_TONL_TX_ID IS NOT NULL) --AND (@RRC_LEASE_ID IS NOT NULL)
        BEGIN TRY 
          BEGIN TRAN   
            UPDATE [PAR_UPS16_QRA].[dbo].TONL_TX_CPA_MST SET 
              /*1*/  DRL_PRMT_NO	= ISNULL(@DRILLING_PERMIT_NUMBER, DRL_PRMT_NO),  
              /*2*/  LSE_NO			= CASE WHEN (@EXISTING_LEASE_ID <> ISNULL(@RRC_LEASE_ID, LSE_NO)) AND (@WELL_CLASS_CHANGED = 1) THEN REPLACE(STR(LSE_NO,6,0),' ','0')    --IF BOTH THE LEASE NUMBER AND WELL CLASS HAVE CHANGED THEN WE NEED TO KEEP THE LEASE ID FOR THE CURRENT RECORD AND ADD A NEW RECORD BELOW
											WHEN ISNULL(@RRC_LEASE_ID, LSE_NO) IS NOT NULL AND @RRC_LEASE_ID <> '' THEN REPLACE(STR(ISNULL(@RRC_LEASE_ID, LSE_NO),6,0),' ','0')
											WHEN (@RRC_LEASE_ID IS NULL OR @RRC_LEASE_ID = '') AND @DRILLING_PERMIT_NUMBER IS NOT NULL THEN @DRILLING_PERMIT_NUMBER
											ELSE '' END,  
              /*3*/  DRILL_PRMT_FL	= CASE WHEN @RRC_LEASE_ID IS NOT NULL AND @RRC_LEASE_ID <> '' THEN 'N'
											WHEN (@RRC_LEASE_ID IS NULL OR @RRC_LEASE_ID = '') AND @DRILLING_PERMIT_NUMBER IS NOT NULL THEN 'Y'
											WHEN @DRILLING_PERMIT_NUMBER IS NOT NULL THEN 'Y' 
											ELSE 'N' END,
              /*4*/  LSE_NM			= ISNULL(@REGULATORY_LEASE_NAME, LSE_NM),  --CHANGED FROM WELL NAME TO REGULATORY LEASE NAME IN MDM PHASE 2
              /*5*/  EFF_DT_TO		= CASE WHEN (@EXISTING_LEASE_ID <> ISNULL(@RRC_LEASE_ID, LSE_NO)) AND (@WELL_CLASS_CHANGED = 1) AND (ISNULL(EFF_DT_FROM, @CURRENT_MONTH_START) < @PREVIOUS_MONTH_END)  THEN DATEADD(MILLISECOND, -DATEPART(MILLISECOND, @PREVIOUS_MONTH_END), @PREVIOUS_MONTH_END)  --IF BOTH THE LEASE NUMBER AND WELL CLASS HAVE CHANGED THEN WE NEED TO SET THE END DATE FOR THE CURRENT RECORD AND ADD A NEW RECORD BELOW
											ELSE '9999-12-31 00:00:00.000' END,
              /*6*/  UPDT_DT		= DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
              /*7*/  UPDT_USER		= 'EH_WELL_WB_SP' 
            WHERE TX_CPA_SEQ_NO = @EXISTING_TONL_TX_ID AND
                WELL_NO = @EXISTING_WELL_ID AND
                COMPL_NO = 1 AND
                OPER_BUS_SEG_CD = 'PAR';

            --***** LOG SUCCESS *****  
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum TONL_TX_CPA_MST Table Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            )
          COMMIT TRAN

          --***** WELL WAS UPDATED *****
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** WELL WAS NOT UPDATED *****

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum TONL_TX_CPA_MST Table Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          )

        SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
        RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END
	
    BEGIN  --INSERT NEW STATUS/WELL CLASS RECORD IN PONL_WELL_COMPL_EFF_DT IF PREVIOUS RECORD HAD SET EFF_DATE_TO = YESTERDAY IN THE UPDATE STATEMENT ABOVE  
    --**************************************************************
      IF (@EXISTING_PONL_WELL_COMPL_EFF_ID IS NOT NULL) AND 
          ((@WELL_CLASS_CHANGED = 1) AND (DATEDIFF(DAY, @EXISTING_WELL_CLASS_EFF_DT_FROM, GETDATE()) <> 0) OR
           (@WELL_STATUS_CHANGED = 1))
        BEGIN TRY
          BEGIN TRAN

            --STORE THE CASE STATEMENT'S RESULT IN A VARIABLE FOR USE IN THE ERROR MESSAGE
            DECLARE @STATUS_START_DATE DATETIME = CASE
				  WHEN (@WELL_CLASS_CHANGED = 1) THEN CAST(DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()) AS DATE)  --WELL CLASS CHANGED, DO NOT USE DRILLING-EFFECTIVE DATES FROM PARAMETER SECTION ABOVE
                  WHEN (@WELL_STATUS_CHANGED = 1) THEN @WELL_STATUS_EFFECTIVE_DATE --CAST(DATEADD(MILLISECOND, -DATEPART(MILLISECOND, @STATUS_NEW_START_DATE), @STATUS_NEW_START_DATE) AS DATE)  --WELL CLASS CHANGED, USE DRILLING-EFFECTIVE DATES FROM PARAMETER SECTION ABOVE
                  WHEN (@EXISTING_WELL_STATUS_ID IS NULL) OR (@NEW_WELL_STATUS_ID IS NULL) THEN @WELL_STATUS_EFFECTIVE_DATE --CAST(DATEADD(MILLISECOND, -DATEPART(MILLISECOND, @STATUS_NEW_START_DATE), @STATUS_NEW_START_DATE) AS DATE)
                  ELSE CAST(DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()) AS DATE)
                  END

            --*****CREATE THE PONL_WELL_COMPL_EFF_DT
            INSERT INTO [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT(

              --***** IDENTIFIERS *****
              /*1*/  WELL_NO,  
              /*2*/  COMPL_NO,  

              --***** MDM ATTRIBUTES *****
              /*3*/  WELL_CLASS_CD,    --WELL_CLASS_NAME
              /*4*/  WELL_STAT_CD,    --WELL_STATUS

              --***** SYSTEM ATTRIBUTES *****
              /*5*/  OPER_BUS_SEG_CD,
              /*6*/  EFF_DT_FROM,
              /*7*/  EFF_DT_TO,
              /*8*/  OPER_FL,
              /*9*/  UPDT_USER,
              /*10*/ UPDT_DT,
              /*11*/ EXP_FL
            ) 
            VALUES
            (  
              --***** IDENTIFIERS *****
              /*1*/  @EXISTING_WELL_ID,
              /*2*/  1,

              --***** MDM ATTRIBUTES *****
              /*3*/  @WELL_CLASS_NAME,
              /*4*/   CASE WHEN (@WELL_STATUS_CHANGED = 1) THEN ISNULL(@NEW_WELL_STATUS_ID, '2020') --WELL STATUS CHANGED, USE NEW STATUS CODE
                  ELSE ISNULL(@EXISTING_WELL_STATUS_ID, '2020') --WELL CLASS (NOT WELL STATUS) CHANGED, USE EXISTING STATUS CODE
                  END,
              --***** SYSTEM ATTRIBUTES *****
              /*5*/  'PAR',
              /*6*/  @WELL_STATUS_EFFECTIVE_DATE,
              /*7*/  '9999-12-31 00:00:00.000',
              /*8*/  'Y',  
              /*9*/  'EH_WELL_WB_SP',
              /*10*/ DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
              /*11*/ 'N'
            );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL_COMPL_EFF_DT Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** PONL_WELL_COMPL_EFF_DT WAS CREATED *****
          SET @EXISTING_PONL_WELL_COMPL_EFF_ID = @EXISTING_WELL_ID
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** PONL_WELL_COMPL_EFF_DT WAS NOT CREATED *****
          SET @EXISTING_PONL_WELL_COMPL_EFF_ID = NULL
          SET @ERROR_MESSAGE = ERROR_MESSAGE();
		  SET @ERROR_STATUS_DUPLICATE = 1;

          IF @ERROR_MESSAGE LIKE '%Cannot insert duplicate key in object ''dbo.PONL_WELL_COMPL_EFF_DT''.%'
            BEGIN
              SET @ERROR_MESSAGE = 'DUPLICATE STATUS DATES: THIS WELL ALREADY HAS A STATUS WITH AN EFF_DT_FROM OF [' + ISNULL(CONVERT(VARCHAR(50), @WELL_STATUS_EFFECTIVE_DATE), 'NULL') + '], UNABLE TO INSERT NEW STATUS WITH THE EXACT SAME DATETIME.'
              SET @ERROR_SEVERITY = 16;
              SET @ERROR_STATE = 1;
            END

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL_COMPL_EFF_DT Table Insert Error' + ': ' + @ERROR_MESSAGE, GetDate(), @WELL_LEVEL_TYPE, @UWI
          );

          SELECT @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --INSERT NEW TONL_TX_CPA_MST RECORD IF PREVIOUS RECORD HAD EFF_DATE_TO SET TO PREVIOUS MONTH END IN THE UPDATE STATEMENT ABOVE  
    --**************************************************************
      SET @EXISTING_TONL_TX_EFF_TO = (SELECT EFF_DT_TO FROM [PAR_UPS16_QRA].[dbo].TONL_TX_CPA_MST WHERE TX_CPA_SEQ_NO = @EXISTING_TONL_TX_ID);

      IF (@EXISTING_TONL_TX_ID IS NOT NULL) AND 
        (@RRC_LEASE_ID IS NOT NULL) AND 
        (ISNULL(@EXISTING_LEASE_ID, '') <> ISNULL(@RRC_LEASE_ID, '')) AND 
        (@WELL_CLASS_CHANGED = 1) AND 
        @EXISTING_TONL_TX_EFF_TO <> '9999-12-31 00:00:00.000' --DATEADD(MILLISECOND, -DATEPART(MILLISECOND, @PREVIOUS_MONTH_END), @PREVIOUS_MONTH_END)


        BEGIN TRY
          BEGIN TRAN

          --*****CREATE THE TONL_TX_CPA_MST
          INSERT INTO [PAR_UPS16_QRA].[dbo].TONL_TX_CPA_MST(

            --***** IDENTIFIERS *****
            --/*1*/  TX_CPA_SEQ_NO,  
            /*2*/  WELL_NO,  
            /*3*/  COMPL_NO,

            --***** MDM ATTRIBUTES *****
            /*4*/  DRL_PRMT_NO,    --DRILLING_PERMIT_NUMBER
            /*5*/  LSE_NO,      --RRC_LEASE_ID
            /*6*/  DRILL_PRMT_FL,   --REQUESTED BY MELANIE ON 3/29/19

            --***** SYSTEM ATTRIBUTES *****
            /*7*/  OPER_BUS_SEG_CD,
            /*8*/  EFF_DT_FROM,
            /*9*/  EFF_DT_TO,
            /*10*/ LSE_NM,   
            /*11*/ TAXPR_NO,
            /*12*/ BUS_UNIT_CD,        
            /*13*/ CTRY_CD,
            /*14*/ ST_CD,
            /*15*/ UPDT_USER,
            /*16*/ UPDT_DT,
            /*17*/ PURCH_TAXPR_NO
          ) 
          VALUES
          (  

            --***** IDENTIFIERS *****
            --/*1*/  (SELECT MAX(TX_CPA_SEQ_NO) + 1 FROM [PAR_UPS16_QRA].[dbo].[TONL_TX_CPA_MST]),
            /*2*/  @EXISTING_WELL_ID,
            /*3*/  1,

            --***** MDM ATTRIBUTES *****
            /*4*/  @DRILLING_PERMIT_NUMBER,
            /*5*/  CASE WHEN @RRC_LEASE_ID IS NOT NULL AND @RRC_LEASE_ID <> '' THEN @RRC_LEASE_ID
						WHEN (@RRC_LEASE_ID IS NULL OR @RRC_LEASE_ID = '') AND @DRILLING_PERMIT_NUMBER IS NOT NULL THEN @DRILLING_PERMIT_NUMBER
						ELSE '' END,  --LEFT(ISNULL(@RRC_LEASE_ID, '123456'), 6),
            /*6*/  CASE WHEN @RRC_LEASE_ID IS NOT NULL AND @RRC_LEASE_ID <> '' THEN 'N'
						WHEN (@RRC_LEASE_ID IS NULL OR @RRC_LEASE_ID = '') AND @DRILLING_PERMIT_NUMBER IS NOT NULL THEN 'Y'
						WHEN @DRILLING_PERMIT_NUMBER IS NOT NULL THEN 'Y' 
                  ELSE 'N' END,

            --***** SYSTEM ATTRIBUTES *****
            /*7*/  'PAR',
            /*8*/  DATEADD(MILLISECOND, -DATEPART(MILLISECOND, @CURRENT_MONTH_START), @CURRENT_MONTH_START),
            /*9*/  '9999-12-31 00:00:00.000',
            /*10*/ @REGULATORY_LEASE_NAME,  --CHANGED FROM WELL NAME TO REGULATORY LEASE NAME IN MDM PHASE 2
            /*11*/ '32034745193',
            /*12*/ @EXISTING_OPERATOR_ID,
            /*13*/ 'US',
            /*14*/ @EXISTING_STATE_ID,  
            /*15*/ 'EH_WELL_WB_SP',
            /*16*/ DATEADD(MILLISECOND, -DATEPART(MILLISECOND, GETDATE()), GETDATE()),
            /*17*/ '999999999'
          );

            --***** LOG SUCCESS ***** 
            INSERT INTO [Enerhub].[CallOutAuditLog]
            (
              [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
            )
            VALUES
            (
              1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum TONL_TX_CPA_MST Table Insert Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
            );
          COMMIT TRAN

          --***** TONL_TX_CPA_MST WAS CREATED *****
          
          SET @EXISTING_PONL_WELL_ID = @EXISTING_WELL_ID
        END TRY
        BEGIN CATCH
          ROLLBACK TRAN
          
          --***** TONL_TX_CPA_MST WAS NOT CREATED *****
          SET @EXISTING_PONL_WELL_ID = NULL

          --***** LOG FAILURE ***** 
          INSERT INTO [Enerhub].[CallOutAuditLog]
          (
            [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
          )
          VALUES
          (
            1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum TONL_TX_CPA_MST Table Insert Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
          );

          SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
          RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

        END CATCH
    --**************************************************************
    END

    BEGIN  --FORCE ENERHUB TO RETRY THE UPDATE IF THE BATTERY EXISTED IN THE PUBLISHER BUT NOT QUORUM  
      IF  (@EXISTING_WELL_BATTERY_ASSIGNMENT_ID IS NOT NULL) AND (@BATTERY IS NOT NULL) AND (@BATTERY_SCTRL_ID IS NULL)
          BEGIN
            SET @ERROR_MESSAGE = 'BATTERY [' + ISNULL(@BATTERY, 'NULL') + '] WAS NOT FOUND IN QUORUM, PLEASE CREATE IT.';
            SET @ERROR_SEVERITY = 16;
            SET @ERROR_STATE = 1;
            RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);
          END
    END

  END TRY
  BEGIN CATCH
  
    SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
    RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

  END CATCH

  
    --***** IF STATUS UPDATE WAS A DUPLICATE, SET THE END DATE BACK TO '9999-12-31 00:00:00.000' *****
	BEGIN
		IF @ERROR_STATUS_DUPLICATE = 1
			BEGIN TRY
				BEGIN TRAN
				  UPDATE [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT SET EFF_DT_TO = '9999-12-31 00:00:00.000'
				  WHERE WELL_NO = @EXISTING_WELL_ID AND
						COMPL_NO = 1 AND
						OPER_BUS_SEG_CD = 'PAR' AND
						UPDT_DT = (SELECT MAX(UPDT_DT) FROM [PAR_UPS16_QRA].[dbo].PONL_WELL_COMPL_EFF_DT WHERE OPER_BUS_SEG_CD = 'PAR' AND WELL_NO = @EXISTING_WELL_ID AND COMPL_NO = 1);
						
					--***** LOG SUCCESS *****  
					INSERT INTO [Enerhub].[CallOutAuditLog]
					(
					  [CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
					)
					VALUES
					(
					  1, 1, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL_COMPL_EFF_DT Table EFF_DT_TO Update Successful', GetDate(), @WELL_LEVEL_TYPE, @UWI
					)
				COMMIT TRAN
			END TRY
			BEGIN CATCH
			          
			  --***** STATUS DATE ROLLBACK FAILED *****

			  --***** LOG FAILURE ***** 
			  INSERT INTO [Enerhub].[CallOutAuditLog]
			  (
				[CallOutId], [LevelCode], [WellLevelTypeId], [Description], [CreatedOn], [LevelType], [UWI]
			  )
			  VALUES
			  (
				1, 2, COALESCE(@Well_Version_LoadId, @WELLId), 'Quorum PONL_WELL_COMPL_EFF_DT Table EFF_DT_TO Update Error' + ': ' + Error_Message(), GetDate(), @WELL_LEVEL_TYPE, @UWI
			  )

			  SELECT @ERROR_MESSAGE = ERROR_MESSAGE(), @ERROR_SEVERITY = ERROR_SEVERITY(), @ERROR_STATE = ERROR_STATE();
			  RAISERROR (@ERROR_MESSAGE, @ERROR_SEVERITY, @ERROR_STATE);

			END CATCH

	END

END
GO



/*
*****READ ME*****
DESCRIPTION: MJE POPULATION INCLUDING CODING DETAILS AND APPROVE/POST USERS
DATABASE: PAR_UPS16_QCFS
DYNAMIC PARAMETERS:
	1. @ACCT_MO: ACCOUNTING MONTH
	2. @BU_USERKEY_LIST: LIST OF COMPANIES ***MUST BE IN FORMAT: ('001'), ('015'), ('101'), ('115')
	3. @FS_TYPE_CONSOL: CONSOLIDATED FINANCIAL STATEMENT TYPE (i.e. GA, OGPRO, LOEXP, etc.)
*/

DECLARE @ACCT_MO datetime;
DECLARE @BU_USERKEY_LIST table (id varchar(50));
--DECLARE @FS_TYPE_CONSOL varchar(50);

--SET PARAMETERS HERE
SET @ACCT_MO = '4/1/2019';
INSERT @BU_USERKEY_LIST(id) values ('002'), ('102'), ('015'), ('115'),('020'),('120'); 
--SET @FS_TYPE_CONSOL = 'GA';

SELECT *
FROM
(

SELECT ROW_NUMBER() OVER(ORDER BY BATCHUSERKEY, BATCH_NAME, REFNUM, DESCRIPTION, JE_SOURCE, BUS_UNIT_CD, BU_NAME, ACCOUNT_NO, GEN, SUB, ACCOUNT_CAT, FSTYPE_CONSOL,
ACCT_NAME, AMOUNT, DATEACCT, DATEACTIVITY, PROP_NO, PROP_NM, APPROVE_DATE,
APPROVE_USER, POST_DATE, POST_USER, VENDOR_NO, INVOICE_NO, IS_JIB_REBILL ASC) AS ROWNUM, *
FROM
(
SELECT M.BATCHUSERKEY,  
CASE WHEN CM.NAME IS NOT NULL THEN CM.NAME WHEN VM.NAME IS NOT NULL THEN VM.NAME WHEN IM.NAME IS NOT NULL THEN IM.NAME 
WHEN PM.NAME IS NOT NULL THEN PM.NAME WHEN DM.NAME IS NOT NULL THEN DM.NAME WHEN JM.NAME IS NOT NULL THEN JM.NAME ELSE 'NO BATCH NAME' END AS BATCH_NAME,
M.REFNUM, M.DESCRIPTION, M.JOURNALDEF JE_SOURCE, BE.BUS_UNIT_CD, M.BU_NAME, M.KEYFULL ACCOUNT_NO, LEFT(M.KEYFULL, 4) GEN, RIGHT(LEFT(M.KEYFULL, 9), 4) SUB,
CASE WHEN RIGHT(M.KEYFULL,2) = '00' THEN 'N/A' WHEN RIGHT(M.KEYFULL,2) = '01' THEN 'DRILLING' WHEN RIGHT(M.KEYFULL,2) IN ('02','20','21','22') THEN 'COMPLETIONS'
WHEN RIGHT(M.KEYFULL,2) IN ('03','31','32','33','34','35','36','37') THEN 'FACILITIES' WHEN RIGHT(M.KEYFULL,2) = '04' THEN 'LEASE OPERATING EXPENSE'  
WHEN RIGHT(M.KEYFULL,2) = '05' THEN 'WORKOVER' WHEN RIGHT(M.KEYFULL,2) IN ('06','61','63') THEN 'CORPORATE' WHEN RIGHT(M.KEYFULL,2) = '07' THEN 'G&G' WHEN RIGHT(M.KEYFULL,2) = '08' THEN 'PLUG AND ABANDONMENT'
ELSE 'NO ACCOUNT CATEGORY' END AS ACCOUNT_CAT, FSTC.NAME FSTYPE_CONSOL,
M.NAME ACCT_NAME, SUM(M.AMOUNT) AMOUNT,
M.DATEACCT, M.DATEACTIVITY, COALESCE(M.COSTCENTERCD, M.PROP_NO) PROP_NO, CC.COST_CNTR_DESCR PROP_NM,
CASE WHEN APPROVE1.APPROVE_DATE IS NULL THEN '1/1/1900' ELSE  APPROVE1.APPROVE_DATE END AS APPROVE_DATE, 
CASE WHEN APPROVE2.AD_USER IS NULL THEN 'NO APPROVE USER' ELSE APPROVE2.AD_USER END AS APPROVE_USER, 
CASE WHEN POST1.POST_DATE IS NULL THEN '1/1/1900' ELSE POST1.POST_DATE END AS POST_DATE, 
CASE WHEN POST2.AD_USER IS NULL THEN 'NO POST USER' ELSE POST2.AD_USER END AS POST_USER ,
CASE WHEN M.VENDOR_BA_NO IS NOT NULL THEN M.VENDOR_BA_NO WHEN M.SRC_VENDOR_NO IS NOT NULL THEN M.SRC_VENDOR_NO WHEN M.OWNER_NO IS NOT NULL THEN M.OWNER_NO ELSE NULL END AS VENDOR_NO,
 M.VENDOR_BA_NO,
M.SRC_VENDOR_NO,
M.OWNER_NO,
CASE WHEN M.INVOICE_NO IS NOT NULL THEN M.INVOICE_NO WHEN M.SRC_INVOICE_NO IS NOT NULL THEN M.SRC_INVOICE_NO WHEN M.VENDOR_CTR_NO IS NOT NULL THEN M.VENDOR_CTR_NO ELSE NULL END AS INVOICE_NO,
CASE WHEN EOMONTH(M.SRC_DATEACCT) < EOMONTH(M.DATEACCT) THEN 'Y' ELSE 'N' END AS IS_JIB_REBILL
FROM PAR_CUSTOM_UVW_UPS_JETRANSACTIONINQUIRY M
JOIN PAR_UPS16_QCFS..BUSINESSENTITY BE
ON M.IDBEOWNER = BE.ID
JOIN PAR_UPS16_QCFS..ACCOUNT A
ON M.IDACCOUNT = A.ID
LEFT JOIN PAR_UPS16_QCFS..SCTRL_COST_CNTR CC
ON COALESCE(M.COSTCENTERCD, M.PROP_NO) = CC.COST_CNTR_CD
LEFT JOIN PAR_UPS16_QCFS..FINANCIALSTATEMENTTYPE FSTC
ON A.IDFSTYPECONSOL = FSTC.ID
LEFT JOIN PAR_UPS16_QCFS..BATCHCHECKRUNMASTER CM
ON M.BATCHUSERKEY = CM.USERKEY
AND M.IDSRCBEOWNER = CM.IDBEOWNER
LEFT JOIN PAR_UPS16_QCFS..BATCHVOUCHERMASTER VM
ON M.BATCHUSERKEY = VM.USERKEY
AND M.IDSRCBEOWNER = VM.IDBEOWNER
LEFT JOIN PAR_UPS16_QCFS..BATCHINVOICEMASTER IM
ON M.BATCHUSERKEY = IM.USERKEY
AND M.IDSRCBEOWNER = IM.IDBEOWNER
LEFT JOIN PAR_UPS16_QCFS..BATCHPAYMENTAPPLICATIONMASTER PM
ON M.BATCHUSERKEY = PM.USERKEY
AND M.IDSRCBEOWNER = PM.IDBEOWNER
LEFT JOIN PAR_UPS16_QCFS..BATCHDEPOSITWITHMATCHMASTER DM
ON M.BATCHUSERKEY = DM.USERKEY
AND M.IDSRCBEOWNER = DM.IDBEOWNER
LEFT JOIN PAR_UPS16_QCFS..BATCHJOURNALENTRYMASTER JM
ON M.BATCHUSERKEY = JM.USERKEY
AND M.IDSRCBEOWNER = JM.IDBEOWNER
LEFT JOIN (SELECT APPROVE_IDBEOWNER, 
                                             --POST_USER,
                                             MAX(APPROVE_DATE) APPROVE_DATE, 
                                             APPROVE_USERKEY 
											 FROM( 
											 SELECT  
                                             IDBEOWNER AS APPROVE_IDBEOWNER, 
                                             AD_USER AS APPROVE_USER,
                                             CREATED AS APPROVE_DATE, 
                                             USERKEY AS APPROVE_USERKEY
               FROM PAR_UPS16_QCFS..AUDITWORKFLOWLOG
               WHERE ORIGWORKFLOWSTATUS = '10'
			   AND NEWWORKFLOWSTATUS = '70'
			   AND AD_USER NOT IN ('QPEC_SCHEDULER')
               ) APPROVE_INNER
			   GROUP BY APPROVE_IDBEOWNER, APPROVE_USERKEY) APPROVE1
			   ON M.IDSRCBEOWNER = APPROVE1.APPROVE_IDBEOWNER
				AND M.BATCHUSERKEY = APPROVE1.APPROVE_USERKEY
			   LEFT JOIN PAR_UPS16_QCFS..AUDITWORKFLOWLOG APPROVE2
			   ON APPROVE1.APPROVE_DATE = APPROVE2.CREATED
			   AND APPROVE1.APPROVE_IDBEOWNER = APPROVE2.IDBEOWNER
			   AND APPROVE1.APPROVE_USERKEY = APPROVE2.USERKEY
			   AND APPROVE2.ORIGWORKFLOWSTATUS = '10'
			   AND APPROVE2.NEWWORKFLOWSTATUS = '70'

LEFT JOIN (SELECT POST_IDBEOWNER, 
                                             --POST_USER,
                                             MAX(POST_DATE) POST_DATE, 
                                             POST_USERKEY 
											 FROM( 
											 SELECT  
                                             IDBEOWNER AS POST_IDBEOWNER, 
                                             AD_USER AS POST_USER,
                                             CREATED AS POST_DATE, 
                                             USERKEY AS POST_USERKEY
               FROM PAR_UPS16_QCFS..AUDITWORKFLOWLOG
               WHERE NEWWORKFLOWSTATUS IN ('90', '100')
			   AND AD_USER NOT IN ('QPEC_SCHEDULER')
               ) POST_INNER
			   GROUP BY POST_IDBEOWNER, POST_USERKEY) POST1
			   ON M.IDSRCBEOWNER = POST1.POST_IDBEOWNER
AND M.BATCHUSERKEY = POST1.POST_USERKEY
			   LEFT JOIN PAR_UPS16_QCFS..AUDITWORKFLOWLOG POST2
			   ON POST1.POST_DATE = POST2.CREATED
			   AND POST1.POST_IDBEOWNER = POST2.IDBEOWNER
			   AND POST1.POST_USERKEY = POST2.USERKEY
			   AND POST2.NEWWORKFLOWSTATUS IN ('90','100')
WHERE 
--AND FSTC.NAME = 'OIL AND NATURAL GAS PROPERTIES'
coalesce(M.INVOICE_NO, M.INVOICE_NO, M.SRC_INVOICE_NO,M.VENDOR_CTR_NO) = 'IN888057'
GROUP BY M.BATCHUSERKEY, CM.NAME,VM.NAME,IM.NAME,PM.NAME,DM.NAME,JM.NAME,M.REFNUM, M.DESCRIPTION, M.JOURNALDEF, BE.BUS_UNIT_CD, M.BU_NAME, M.KEYFULL, LEFT(M.KEYFULL, 4), RIGHT(LEFT(M.KEYFULL, 9), 4), FSTC.NAME,
M.NAME, M.DATEACCT, M.DATEACTIVITY, M.COSTCENTERCD, M.PROP_NO, CC.COST_CNTR_DESCR,APPROVE1.APPROVE_DATE,APPROVE2.AD_USER,POST1.POST_DATE,POST2.AD_USER,M.VENDOR_BA_NO,M.SRC_VENDOR_NO,M.OWNER_NO, 
M.INVOICE_NO,M.SRC_INVOICE_NO,M.VENDOR_CTR_NO, M.SRC_DATEACCT ) A
) B
--WHERE INVOICE_NO = 'IN888057'

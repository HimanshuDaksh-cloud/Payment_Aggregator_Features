WITH EBPPFeatures AS (
  SELECT 
    "PaymentIDBillerIDMerchantOrder",
    "What EBPP are we replacing?" AS original_ebpp,
    UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) AS ebpp_clean,

    -- Unknown EBPP check
    CASE WHEN UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) NOT IN (
        'KUBRA','NO EBPP TODAY','COLORADO PAYPORT','HARRIS MYGOVHUB','WELLS FARGO','AUTHORITY PAY',
        'REVTRAK','ACI WORLDWIDE','ALACRITI','EPAY','TYLER EAGLEWEB','FIS-PAYDIRECT',
        'HARRIS ERP-CITIZEN ACCESS','DIVDAT','STURGIS','PAYMENTUS','JETPAY','INFOSEND',
        'WESTERN UNION','D&T VENTURES','REVSPRING','NEXBILLPAY','STARNIK','GOV PAY',
        'WEBSTER','OSG','SPRINGBROOK','CERTIFIED PAYMENTS','BDS','XPRESSBILLPAY',
        'COMMERCIAL PAYMENTS','SPEEDPAY','EDMUNDS WIPP','MUNI-LINK','US BANK','PAYBILL',
        'PAYSTAR','AMS','DATAMATX/DOCSIGHT','QS1','SELECTRON','IN-HOUSE GATEWAY',
        'E-Z PAY','FISERV','MYGOVHUB','SMARTPAY','HARRIS-NORTHSTAR ECARE','TYLER ESUITE',
        'BRITECORE','XPRESS-PAY','NCOURT','MCC','PAYROC','HARRIS ICONNECT','CSG',
        'IL EPAY','UNI PAY','TRANSACTION WAREHOUSE','MERCHANT TRANSACT','STW',
        'HEARTLAND PAYMENTS','CITYBASE','MUNICIPAY','LEVEL ONE/VITALCHEK','PSN',
        'G2G OAKLAND COUNTY','ENETPAY','BOFA VELOCITY','TYLER MUNICIPAL ONLINE PAYMENTS',
        'GOVERNMENT WINDOW','ELAVON','OFFICIAL PAYMENTS','TRUE POINT SOLUTIONS',
        'BLUEFIN PAYMENT SYSTEMS','COLLECTOR SOLUTIONS/JETPAY','SEW','NCR',
        'CENTRAL SQUARE','QBILLPAY','NIC SERVICES','BILLTRUST','SEDC',
        'INSTANT PAYMENTS','ALL PAID','WORLDPAY','ICONNECT','TYLER','TYLER WEBPAY',
        'PAYCLIX','POINT & PAY','GREENPAY','CONTINENTAL','AUTHORIZE.NET','INFINITY.LINK',
        'DATA WEST','UNITED SYSTEMS','MERCHANT SERVICES','FORTE PAYMENT SYSTEMS','UNKNOWN',
        'EB2GOV','CHASE PAYCONNEXION/CONNECT','CUSI','POWERPAY','PAYPAL GATEWAY',
        'BILLMATRIX','SMART BILL','CITIZEN SELF SERVICE','TYLER MUNIS/CITIZEN SELF-SERVICE',
        'VALUE PAYMENTS','CLICK2GOV','GTS - GOVTECH SERVICES, INC','PAY GOV','VDS'
    ) THEN 1 ELSE 0 END AS is_unknown_ebpp,

    -- Risk tier classification
    CASE WHEN UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) IN (
        'WELLS FARGO','US BANK','CHASE PAYCONNEXION/CONNECT','BOFA VELOCITY',
        'PAYPAL GATEWAY','FISERV','ACI WORLDWIDE','FIS-PAYDIRECT','WESTERN UNION',
        'WORLDPAY','ELAVON','HEARTLAND PAYMENTS','AUTHORIZE.NET'
    ) THEN 1 ELSE 0 END AS ebpp_tier1,

    CASE WHEN UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) IN (
        'OFFICIAL PAYMENTS','PAY GOV','PAYMENTUS','TYLER','TYLER EAGLEWEB',
        'TYLER ESUITE','TYLER WEBPAY','TYLER MUNICIPAL ONLINE PAYMENTS',
        'TYLER MUNIS/CITIZEN SELF-SERVICE','BILLMATRIX','BILLTRUST','REVTRAK',
        'SPEEDPAY','POINT & PAY','FORTE PAYMENT SYSTEMS','CSG','KUBRA',
        'CENTRAL SQUARE','NIC SERVICES','NCR'
    ) THEN 1 ELSE 0 END AS ebpp_tier2,

    CASE WHEN UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) IN (
        'SMARTPAY','GREENPAY','ENETPAY','JETPAY','NEXBILLPAY','PAYBILL','PAYSTAR',
        'STARNIK','MUNI-LINK','QS1','SEW','VDS','STURGIS','DIVDAT','INFOSEND',
        'D&T VENTURES','XPRESSBILLPAY','PAYCLIX','INFINITY.LINK','DATA WEST',
        'UNITED SYSTEMS','QBILLPAY','INSTANT PAYMENTS','ALL PAID','POWERPAY'
    ) THEN 1 ELSE 0 END AS ebpp_tier3,

    -- Special patterns
    CASE WHEN UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%IN-HOUSE%'
         OR UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%INHOUSE%'
         OR UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%INTERNAL%'
         THEN 1 ELSE 0 END AS is_inhouse_gateway,

    CASE WHEN UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%GOV%'
         OR UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%MUNICIPAL%'
         OR UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%CITY%'
         OR UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%COUNTY%'
         THEN 1 ELSE 0 END AS is_gov_provider,

    CASE WHEN UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%TEMP%'
         OR UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%TRANSITION%'
         OR UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) = 'UNKNOWN'
         OR UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%TEST%'
         OR UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) LIKE '%PILOT%'
         OR UPPER(TRIM(COALESCE("What EBPP are we replacing?", ''))) = 'NO EBPP TODAY'
         THEN 1 ELSE 0 END AS is_temp_provider
  FROM 
    CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
)

SELECT
  "PaymentIDBillerIDMerchantOrder",
  original_ebpp AS "What EBPP are we replacing?",
  ebpp_clean,
  is_unknown_ebpp,
  ebpp_tier1,
  ebpp_tier2,
  ebpp_tier3,
  is_inhouse_gateway,
  is_gov_provider,
  is_temp_provider,
  
  -- EBPP risk score
  (CASE WHEN is_unknown_ebpp = 1 THEN 7 ELSE 0 END) +
  (CASE WHEN ebpp_tier1 = 1 THEN -2 ELSE 0 END) +
  (CASE WHEN ebpp_tier3 = 1 THEN 3 ELSE 0 END) +
  (CASE WHEN is_inhouse_gateway = 1 THEN 4 ELSE 0 END) +
  (CASE WHEN is_temp_provider = 1 THEN 6 ELSE 0 END)
  AS ebpp_risk_score,
  
  -- EBPP replacement recommendation
  CASE
    WHEN (
      (CASE WHEN is_unknown_ebpp = 1 THEN 7 ELSE 0 END) +
      (CASE WHEN ebpp_tier1 = 1 THEN -2 ELSE 0 END) +
      (CASE WHEN ebpp_tier3 = 1 THEN 3 ELSE 0 END) +
      (CASE WHEN is_inhouse_gateway = 1 THEN 4 ELSE 0 END) +
      (CASE WHEN is_temp_provider = 1 THEN 6 ELSE 0 END)
    ) >= 6 THEN 'High Priority Replacement'
    WHEN (
      (CASE WHEN is_unknown_ebpp = 1 THEN 7 ELSE 0 END) +
      (CASE WHEN ebpp_tier1 = 1 THEN -2 ELSE 0 END) +
      (CASE WHEN ebpp_tier3 = 1 THEN 3 ELSE 0 END) +
      (CASE WHEN is_inhouse_gateway = 1 THEN 4 ELSE 0 END) +
      (CASE WHEN is_temp_provider = 1 THEN 6 ELSE 0 END)
    ) >= 3 THEN 'Consider Replacement'
    WHEN (
      (CASE WHEN is_unknown_ebpp = 1 THEN 7 ELSE 0 END) +
      (CASE WHEN ebpp_tier1 = 1 THEN -2 ELSE 0 END) +
      (CASE WHEN ebpp_tier3 = 1 THEN 3 ELSE 0 END) +
      (CASE WHEN is_inhouse_gateway = 1 THEN 4 ELSE 0 END) +
      (CASE WHEN is_temp_provider = 1 THEN 6 ELSE 0 END)
    ) > 0 THEN 'Monitor'
    ELSE 'No Action Needed'
  END AS ebpp_replacement_recommendation

FROM 
  EBPPFeatures
  WHERE "What EBPP are we replacing?" is not NULL
ORDER BY 
  ebpp_risk_score DESC;
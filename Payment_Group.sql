WITH Cleaned AS (
  SELECT
   "PaymentIDBillerIDMerchantOrder",
    "PaymentSourceDesc"            AS raw_channel,
    TRIM(COALESCE("PaymentSourceDesc", ''))      AS channel_clean,
    UPPER(TRIM(COALESCE("PaymentSourceDesc", ''))) AS channel_normalized,
    "CustomerID"                   AS CUSTOMER_ID
  FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
),

ChannelFeatures AS (
  SELECT
    "PaymentIDBillerIDMerchantOrder",
    raw_channel,
    channel_clean,
    channel_normalized,
    CUSTOMER_ID,

    -- presence & length
    CASE WHEN channel_clean = '' THEN 1 ELSE 0 END                 AS missing_channel,
    LENGTH(channel_clean)                                          AS channel_length,

    -- payment group
    CASE 
      WHEN channel_normalized IN ('AUTO PAY','SCHEDULED PAYMENT','RECURRING SCHEDULED PAYMENT') 
        THEN 'Recurring Programs'
      WHEN channel_normalized IN ('SHOPPING CART','CUSTOMER PORTAL','EXPRESS PAYMENTS','BILLER PORTAL','BILLER PORTAL - EASY PAY') 
        THEN 'Web Self Service'
      WHEN channel_normalized IN ('MOBILE EXPRESS PAYMENTS','PAY BY TEXT','CLOUD STORE - CONSUMER INITIATED') 
        THEN 'Mobile & SMS'
      WHEN channel_normalized IN ('WEBSERVICES','CLOUD PAYMENTS','CLOUD STORE - BILLER INITIATED') 
        THEN 'API & Cloud Integrations'
      WHEN channel_normalized IN ('IVR','LIVE AGENT PAYMENT') 
        THEN 'Telephony'
      WHEN channel_normalized IN ('CSR - ONE TIME PAY','AGENT CONNECT') 
        THEN 'Agent Assisted'
      WHEN channel_normalized IN ('POS','KIOSK') 
        THEN 'In Person'
      ELSE 'Other'
    END                                                            AS payment_group,

    -- channel type flags
    CASE WHEN channel_normalized IN (
      'AGENT CONNECT','LIVE AGENT PAYMENT','CSR - ONE TIME PAY',
      'BILLER PORTAL','BILLER PORTAL - EASY PAY',
      'WEBSERVICES','CLOUD STORE - BILLER INITIATED',
      'POS','KIOSK'
    ) THEN 1 ELSE 0 END                                            AS is_business_channel,

    CASE WHEN channel_normalized IN (
      'PAY BY TEXT','SHOPPING CART','MOBILE EXPRESS PAYMENTS',
      'CLOUD PAYMENTS','CUSTOMER PORTAL','AUTO PAY',
      'RECURRING SCHEDULED PAYMENT','EXPRESS PAYMENTS',
      'IVR','SCHEDULED PAYMENT','CLOUD STORE - CONSUMER INITIATED'
    ) THEN 1 ELSE 0 END                                            AS is_consumer_channel,

    CASE WHEN channel_normalized IN (
      'PAY BY TEXT','SHOPPING CART','MOBILE EXPRESS PAYMENTS',
      'CLOUD PAYMENTS','BILLER PORTAL','WEBSERVICES',
      'CUSTOMER PORTAL','AUTO PAY','POS','RECURRING SCHEDULED PAYMENT',
      'EXPRESS PAYMENTS','IVR','SCHEDULED PAYMENT','LIVE AGENT PAYMENT',
      'CLOUD STORE - BILLER INITIATED','CLOUD STORE - CONSUMER INITIATED',
      'AGENT CONNECT','KIOSK','CSR - ONE TIME PAY','BILLER PORTAL - EASY PAY'
    ) THEN 1 ELSE 0 END                                            AS is_standard_channel,

    CASE WHEN REGEXP_LIKE(channel_normalized, '\\b(TEST|DUMMY|SAMPLE|DEMO|DEV)\\b') 
         THEN 1 ELSE 0 END                                          AS is_test_channel

  FROM Cleaned
),

ChannelCounts AS (
  SELECT
    channel_normalized AS channel,
    COUNT(*)            AS channel_frequency,
    COUNT(DISTINCT CUSTOMER_ID) AS distinct_customers
  FROM Cleaned
  WHERE channel_clean <> ''
  GROUP BY channel_normalized
),

GroupCounts AS (
  SELECT
    payment_group,
    COUNT(*)                    AS group_frequency,
    COUNT(DISTINCT CUSTOMER_ID) AS group_distinct_customers
  FROM ChannelFeatures
  WHERE channel_clean <> ''
  GROUP BY payment_group
),

ChannelRiskFeatures AS (
  SELECT
    cf.*,
    COALESCE(cc.channel_frequency,0)        AS channel_frequency,
    COALESCE(cc.distinct_customers,0)       AS distinct_customers,
    COALESCE(gc.group_frequency,0)          AS group_frequency,
    COALESCE(gc.group_distinct_customers,0) AS group_distinct_customers,

    -- outlier within group
    CASE 
      WHEN cc.channel_frequency * 10 < gc.group_frequency
           AND cc.channel_frequency < 10
           AND cf.payment_group <> 'Other'
      THEN 1 ELSE 0 
    END                                      AS is_group_outlier,

    -- custom vs standard
    CASE WHEN cf.is_standard_channel = 0 
              AND cf.missing_channel = 0 
         THEN 1 ELSE 0 END                  AS is_custom_channel,

    -- data-quality flags
    CASE WHEN REGEXP_LIKE(cf.channel_clean, '[^[:alnum:][:space:]\\-_]') 
         THEN 1 ELSE 0 END                  AS has_special_chars,
    CASE WHEN cf.channel_clean = UPPER(cf.channel_clean) 
              AND cf.channel_clean <> '' 
         THEN 1 ELSE 0 END                  AS is_all_caps,

    -- numbers with context
    CASE 
      WHEN REGEXP_LIKE(cf.channel_clean,'[0-9]') 
           AND cf.payment_group IN ('Mobile & SMS','Web Self Service','Recurring Programs') THEN 1
      WHEN REGEXP_LIKE(cf.channel_clean,'[0-9]') 
           AND cf.payment_group IN ('API & Cloud Integrations','Agent Assisted')   THEN 0
      ELSE CASE WHEN REGEXP_LIKE(cf.channel_clean,'[0-9]') THEN 1 ELSE 0 END
    END                                      AS has_numbers_with_group_context,

    CASE WHEN REGEXP_LIKE(cf.channel_clean,'[0-9]') 
              AND cf.is_business_channel = 0 
         THEN 1 ELSE 0 END                  AS has_numbers_not_business,

    -- extreme lengths
    CASE WHEN LENGTH(cf.channel_clean) < 3  AND cf.missing_channel = 0 THEN 1 ELSE 0 END AS channel_too_short,
    CASE WHEN LENGTH(cf.channel_clean) > 50 THEN 1 ELSE 0 END                               AS channel_too_long,

    -- keyboard patterns
    CASE WHEN REGEXP_LIKE(LOWER(cf.channel_clean), '\\b(asdf|qwer|zxcv|1234|wasd)\\b') 
         THEN 1 ELSE 0 END                  AS has_keyboard_pattern
  FROM ChannelFeatures cf
  LEFT JOIN ChannelCounts cc 
    ON cf.channel_normalized = cc.channel
  LEFT JOIN GroupCounts gc 
    ON cf.payment_group     = gc.payment_group
)

SELECT
crf."PaymentIDBillerIDMerchantOrder",
  crf.raw_channel,
  crf.channel_clean,
  crf.payment_group,
  crf.missing_channel,
  crf.is_business_channel,
  crf.is_consumer_channel,
  crf.is_standard_channel,
  crf.is_test_channel,
  crf.is_custom_channel,
  crf.has_special_chars,
  crf.is_all_caps,
  crf.has_numbers_not_business,
  crf.has_numbers_with_group_context,
  crf.channel_too_short,
  crf.channel_too_long,
  crf.has_keyboard_pattern,

  crf.channel_frequency,
  crf.distinct_customers,
  CASE WHEN crf.channel_frequency < 5 THEN 1 ELSE 0 END AS is_rare_channel,

  crf.group_frequency,
  crf.group_distinct_customers,
  crf.is_group_outlier,

  (
    crf.channel_too_short * CASE WHEN crf.is_business_channel=1 THEN 1 ELSE 3 END +
    crf.is_custom_channel * CASE WHEN crf.is_business_channel=1 THEN 2 ELSE 5 END +
    CASE WHEN crf.payment_group='Other' THEN 4 ELSE 0 END +
    CASE WHEN crf.payment_group='Other' AND crf.is_custom_channel=1 THEN 3 ELSE 0 END +
    crf.missing_channel*10 +
    crf.channel_too_long*1 +
    crf.is_all_caps*2 +
    crf.is_test_channel*10 +
    crf.has_special_chars*2 +
    crf.has_keyboard_pattern*8 +
    crf.has_numbers_with_group_context*3 +
    CASE WHEN crf.channel_frequency<5 THEN 6 ELSE 0 END +
    CASE WHEN crf.distinct_customers<3 AND crf.channel_frequency>0 THEN 4 ELSE 0 END +
    CASE WHEN crf.is_group_outlier=1 THEN 5 ELSE 0 END
  ) AS channel_risk_score,

  CASE 
    WHEN channel_risk_score <=  3 THEN 'Very Low'
    WHEN channel_risk_score <=  7 THEN 'Low'
    WHEN channel_risk_score <= 15 THEN 'Medium'
    ELSE                             'High'
  END AS channel_risk_category,

  CASE
    WHEN crf.missing_channel=1 OR crf.is_test_channel=1 THEN 'Type 1'
    WHEN (crf.is_custom_channel=1
          OR crf.has_numbers_with_group_context=1
          OR crf.has_keyboard_pattern=1
          OR crf.is_group_outlier=1)
         AND crf.payment_group <> 'API & Cloud Integrations'
      THEN 'Type 2'
    WHEN (crf.is_custom_channel=1 
          OR REGEXP_LIKE(crf.channel_clean,'[0-9]') 
          OR crf.has_keyboard_pattern=1)
         AND crf.payment_group IN ('API & Cloud Integrations','Agent Assisted')
      THEN 'Type 3'
    ELSE 'Type 4'
  END AS channel_type

FROM ChannelRiskFeatures crf
ORDER BY channel_risk_score DESC;
 
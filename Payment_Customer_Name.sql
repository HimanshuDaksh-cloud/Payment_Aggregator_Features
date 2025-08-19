WITH Base AS (
  SELECT
    "Payment_CustomerName"                                               AS original_name,
    
"PaymentIDBillerIDMerchantOrder" ,   -- normalize nulls → '', trim, collapse internal whitespace
    REGEXP_REPLACE(
      TRIM(COALESCE("Payment_CustomerName", '')),
      '\\s+',
      ' '
    )                                                                     AS norm_name
  FROM
    CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
),

NameCounts AS (
  SELECT
    UPPER(norm_name)                                                     AS norm_key,
    COUNT(*)                                                             AS name_frequency
  FROM Base
  GROUP BY UPPER(norm_name)
),

NameFeatures AS (
  SELECT
    "PaymentIDBillerIDMerchantOrder",
    original_name,
    norm_name,
    UPPER(norm_name)                                                     AS norm_upper,
    LOWER(norm_name)                                                     AS norm_lower,

    -- length
    LENGTH(norm_name)                                                    AS name_length,
    CASE WHEN norm_name <> '' AND LENGTH(norm_name) < 4 THEN 1 ELSE 0 END AS name_too_short,
    CASE WHEN LENGTH(norm_name) > 50 THEN 1 ELSE 0 END                   AS name_too_long,

    -- words
    REGEXP_COUNT(norm_name, '\\S+')                                      AS name_word_count,
    CASE WHEN REGEXP_COUNT(norm_name, '\\S+') = 1 THEN 1 ELSE 0 END       AS single_word_name,

    -- all-caps if it equals its uppercase form AND contains at least one letter
    CASE 
      WHEN norm_name <> '' 
        AND norm_name = UPPER(norm_name) 
        AND REGEXP_LIKE(norm_name, '[A-Za-z]')
      THEN 1 ELSE 0
    END                                                                   AS is_all_caps,

    -- all-lower similarly
    CASE 
      WHEN norm_name <> '' 
        AND norm_name = LOWER(norm_name) 
        AND REGEXP_LIKE(norm_name, '[A-Za-z]')
      THEN 1 ELSE 0
    END                                                                   AS is_all_lowercase,

    -- business-entity (word-boundary)
    CASE WHEN REGEXP_LIKE(
           norm_upper,
           '\\b(LLC|L\\.L\\.C|LLP|L\\.L\\.P|INC\\.?|CORP\\.?|LTD\\.?|LIMITED|COMPANY|CO\\.?|GMBH|PLC|S\\.A\\.?|A\\.G\\.?|N\\.V\\.?|PVT\\. LTD\\.?|PTY\\. LTD\\.?)\\b'
         ) THEN 1 ELSE 0 END                                              AS is_business_entity,

    -- exact “test” names
    CASE WHEN LOWER(norm_name) IN (
           'test','testing','test test','john doe','jane doe','john smith',
           'test user','user test','demo','sample','user','customer','anonymous',
           'admin','administrator'
         ) THEN 1 ELSE 0 END                                               AS is_test_name,

    -- test-keyword anywhere (use lowercase norm and simple word-bounds)
    CASE WHEN REGEXP_LIKE(
           norm_lower,
           '\\b(test|dummy|fake|sample|demo)\\b'
         ) THEN 1 ELSE 0 END                                              AS contains_test_keyword,

    -- special chars (ASCII-only)
    CASE WHEN REGEXP_LIKE(
           norm_name,
           '[^A-Za-z0-9\\s\\.,\\-\\'']'
         ) THEN 1 ELSE 0 END                                              AS has_special_chars,

    -- digits
    CASE WHEN REGEXP_LIKE(norm_name, '[0-9]') THEN 1 ELSE 0 END          AS has_numbers,

    -- first & last words
    REGEXP_SUBSTR(norm_name, '^\\S+')                                   AS first_name,
    REGEXP_SUBSTR(norm_name, '\\S+$')                                   AS last_name

  FROM Base
),

Scored AS (
  SELECT
    nf.*,
    COALESCE(nc.name_frequency, 0)                                      AS name_frequency,
    CASE WHEN COALESCE(nc.name_frequency, 0) > 1 THEN 1 ELSE 0 END      AS multiple_transactions_same_name,

    -- derived
    CASE WHEN nf.is_business_entity = 0 AND nf.has_numbers = 1 THEN 1 ELSE 0 END     AS has_numbers_not_business,
    CASE WHEN REGEXP_LIKE(nf.norm_lower, '(asdf|qwer|zxcv|1234|wasd)') THEN 1 ELSE 0 END  AS has_keyboard_pattern,
    CASE WHEN nf.is_business_entity = 1 AND nf.name_word_count <= 2 THEN 1 ELSE 0 END  AS potential_shell,
    CASE WHEN nf.first_name = nf.last_name AND nf.first_name <> '' THEN 1 ELSE 0 END    AS first_last_name_same,
    CASE WHEN LENGTH(nf.first_name) = 1 THEN 1 ELSE 0 END                               AS single_letter_first,
    CASE WHEN nf.last_name <> '' AND LENGTH(nf.last_name) = 1 THEN 1 ELSE 0 END          AS single_letter_last,

    -- risk score
    (
      CASE WHEN nf.name_too_short            = 1 THEN CASE WHEN nf.is_business_entity = 1 THEN  1 ELSE  3 END ELSE 0 END +
      CASE WHEN nf.single_word_name         = 1 THEN CASE WHEN nf.is_business_entity = 1 THEN  1 ELSE  4 END ELSE 0 END +
      CASE WHEN nf.is_business_entity = 1 AND nf.name_word_count <= 2     THEN 7 ELSE 0 END +
      CASE WHEN nf.name_too_long             = 1 THEN  1 ELSE  0 END +
      CASE WHEN nf.is_all_caps               = 1 THEN  2 ELSE  0 END +
      CASE WHEN nf.is_test_name              = 1 THEN 10 ELSE  0 END +
      CASE WHEN nf.contains_test_keyword     = 1 THEN  7 ELSE  0 END +
      CASE WHEN nf.has_special_chars         = 1 THEN  2 ELSE  0 END +
      CASE WHEN nf.has_numbers               = 1 AND nf.is_business_entity = 0 THEN  3 ELSE  0 END +
      CASE WHEN REGEXP_LIKE(nf.norm_lower, '(asdf|qwer|zxcv|1234|wasd)') THEN  8 ELSE  0 END +
      CASE WHEN nf.first_name = nf.last_name AND nf.first_name <> '' THEN  4 ELSE  0 END +
      CASE WHEN LENGTH(nf.first_name) = 1   THEN  3 ELSE  0 END +
      CASE WHEN nf.last_name <> '' AND LENGTH(nf.last_name) = 1 THEN  3 ELSE  0 END
    )                                                                   AS name_risk_score
  FROM NameFeatures nf
  LEFT JOIN NameCounts nc
    ON nf.norm_upper = nc.norm_key
)

SELECT
"PaymentIDBillerIDMerchantOrder",
  original_name                        AS "Payment_CustomerName",
  name_length,
  name_too_short,
  name_too_long,
  name_word_count,
  single_word_name,
  is_all_caps,
  is_all_lowercase,
  is_business_entity,
  is_test_name,
  contains_test_keyword,
  has_special_chars,
  has_numbers,
  first_name,
  last_name,
  name_frequency,
  multiple_transactions_same_name,
  has_numbers_not_business,
  has_keyboard_pattern,
  potential_shell,
  first_last_name_same,
  single_letter_first,
  single_letter_last,
  name_risk_score,
  CASE
    WHEN name_risk_score <=  3 THEN 'Very Low'
    WHEN name_risk_score <=  7 THEN 'Low'
    WHEN name_risk_score <= 15 THEN 'Medium'
    ELSE                         'High'
  END                                   AS name_risk_category
FROM Scored
ORDER BY name_risk_score DESC;

WITH Base AS (
  SELECT
  "PaymentIDBillerIDMerchantOrder",
    "Address1" AS original_address,
    -- normalize nulls, trim, collapse internal whitespace
    REGEXP_REPLACE(TRIM(COALESCE("Address1", '')), '\\s+', ' ') AS norm_address
  FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
),

AddressCounts AS (
  SELECT
    UPPER(norm_address) AS norm_key,
    COUNT(*)            AS address_frequency
  FROM Base
  WHERE norm_address <> ''
  GROUP BY UPPER(norm_address)
),

AddressFeatures AS (
  SELECT
    "PaymentIDBillerIDMerchantOrder",
    original_address,
    norm_address,
    UPPER(norm_address) AS norm_upper,
    LOWER(norm_address) AS norm_lower,

    -- Basic length & emptiness
    LENGTH(norm_address)                                             AS address_length,
    CASE WHEN norm_address = '' THEN 1 ELSE 0 END                     AS missing_address,
    CASE WHEN norm_address <> '' AND LENGTH(norm_address) < 10 THEN 1 ELSE 0 END AS address_too_short,
    CASE WHEN LENGTH(norm_address) > 150 THEN 1 ELSE 0 END            AS address_too_long,

    -- Word count
    REGEXP_COUNT(norm_address, '\\S+')                                AS address_word_count,
    CASE WHEN REGEXP_COUNT(norm_address, '\\S+') < 3 THEN 1 ELSE 0 END AS too_few_words,

    -- PO Box detection
    CASE WHEN REGEXP_LIKE(
           norm_upper,
           '\\b(PO BOX|P\\.O\\. BOX|POST OFFICE BOX|POBOX)\\b|\\bBOX\\s+\\d'
         ) THEN 1 ELSE 0 END                                          AS is_po_box,

    -- Test/fake keywords
    CASE WHEN REGEXP_LIKE(
           norm_lower,
           '\\b(test|dummy|fake|sample|demo|123 main|example)\\b'
         ) THEN 1 ELSE 0 END                                          AS has_test_address_keyword,

    -- Very generic “123 Main St”
    CASE WHEN REGEXP_LIKE(norm_address, '^\\d{1,4}\\s+MAIN ST\\b')   THEN 1 ELSE 0 END AS is_generic_address,

    -- Invalid characters (ASCII-only whitelist; hyphen at end avoids range error)
    CASE WHEN REGEXP_LIKE(
           norm_address,
           '[^A-Za-z0-9\\s\\.,#/&-]'
         ) THEN 1 ELSE 0 END                                          AS has_invalid_chars,

    -- All-caps (if contains any letters)
    CASE WHEN norm_address <> ''
              AND norm_address = UPPER(norm_address)
              AND REGEXP_LIKE(norm_address, '[A-Za-z]')
         THEN 1 ELSE 0 END                                          AS all_caps,

    -- High-risk US states
    CASE WHEN REGEXP_LIKE(
           norm_upper,
           '\\b(DELAWARE|DE|NEVADA|NV|WYOMING|WY|SOUTH DAKOTA|SD)\\b'
         ) THEN 1 ELSE 0 END                                          AS has_high_risk_state,

    -- Virtual-office indicators
    CASE WHEN REGEXP_LIKE(
           norm_lower,
           '\\b(regus|virtual|spaces|wework|office suite|intelligent office|suite \\d{1,4}[A-Z]?)\\b'
         ) THEN 1 ELSE 0 END                                          AS potential_virtual_office,

    -- Numeric-only
    CASE WHEN REGEXP_LIKE(norm_address, '^\\d+$')                   THEN 1 ELSE 0 END AS numbers_only,

    -- ZIP-code pattern
    CASE WHEN REGEXP_LIKE(norm_address, '\\b\\d{5}(-\\d{4})?\\b')   THEN 1 ELSE 0 END AS has_zip_format,

    -- Keyboard mashing
    CASE WHEN REGEXP_LIKE(norm_lower, '\\b(asdf|qwer|zxcv|1234|wasd)\\b') THEN 1 ELSE 0 END AS has_keyboard_pattern,

    -- Numbers but no business suffix
    CASE WHEN REGEXP_LIKE(norm_address, '[0-9]')
          AND NOT REGEXP_LIKE(
                norm_upper,
                '\\b(LLC|LLP|INC|INC\\.|CORP|CORP\\.|LTD|LTD\\.|LIMITED|COMPANY)\\b'
              )
         THEN 1 ELSE 0 END                                          AS has_numbers_not_business
  FROM Base
),

Scored AS (
  SELECT
    af.*,
    COALESCE(ac.address_frequency, 0)                               AS address_frequency,
    CASE WHEN COALESCE(ac.address_frequency, 0) > 10 THEN 1 ELSE 0 END AS high_frequency_address,

    /* Risk score aggregation */
    (
      CASE WHEN af.missing_address            = 1 THEN  8 ELSE  0 END +
      CASE WHEN af.address_too_short          = 1 THEN  4 ELSE  0 END +
      CASE WHEN af.address_too_long           = 1 THEN  1 ELSE  0 END +
      CASE WHEN af.too_few_words              = 1 THEN  3 ELSE  0 END +
      CASE WHEN af.is_po_box                  = 1 THEN  3 ELSE  0 END +
      CASE WHEN af.has_test_address_keyword   = 1 THEN  8 ELSE  0 END +
      CASE WHEN af.is_generic_address         = 1 THEN  5 ELSE  0 END +
      CASE WHEN af.has_invalid_chars          = 1 THEN  3 ELSE  0 END +
      CASE WHEN af.all_caps                   = 1 THEN  1 ELSE  0 END +
      CASE WHEN af.has_high_risk_state        = 1 THEN  2 ELSE  0 END +
      CASE WHEN af.potential_virtual_office   = 1 THEN  4 ELSE  0 END +
      CASE WHEN af.numbers_only               = 1 THEN  6 ELSE  0 END +
      CASE WHEN af.has_keyboard_pattern       = 1 THEN  7 ELSE  0 END +
      CASE WHEN af.has_zip_format      = 0                THEN  2 ELSE  0 END +
      CASE WHEN af.has_numbers_not_business   = 1 THEN  3 ELSE  0 END +
      CASE WHEN COALESCE(ac.address_frequency, 0) > 10   THEN  4 ELSE  0 END
    )                                                               AS address_risk_score
  FROM AddressFeatures af
  LEFT JOIN AddressCounts ac
    ON af.norm_upper = ac.norm_key
)
SELECT
  "PaymentIDBillerIDMerchantOrder",
  original_address                AS "Address1",
  address_length,
  missing_address,
  address_too_short,
  address_too_long,
  address_word_count,
  too_few_words,
  is_po_box,
  has_test_address_keyword,
  is_generic_address,
  has_invalid_chars,
  all_caps,
  has_high_risk_state,
  potential_virtual_office,
  numbers_only,
  has_zip_format,
  has_keyboard_pattern,
  has_numbers_not_business,
  address_frequency,
  high_frequency_address,
  address_risk_score,
  CASE
    WHEN address_risk_score <=  3 THEN 'Very Low'
    WHEN address_risk_score <=  7 THEN 'Low'
    WHEN address_risk_score <= 15 THEN 'Medium'
    ELSE                             'High'
  END                                AS address_risk_category 
FROM Scored
ORDER BY address_risk_score DESC

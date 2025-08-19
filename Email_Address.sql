WITH Base AS (
  SELECT
  "PaymentIDBillerIDMerchantOrder",
    "EmailAddress" AS original_email,
    LOWER(TRIM(COALESCE("EmailAddress", ''))) AS norm_email
  FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
),

EmailCounts AS (
  SELECT
    norm_email      AS email_key,
    COUNT(*)        AS email_frequency
  FROM Base
  WHERE norm_email <> ''
  GROUP BY norm_email
),

RawParts AS (
  SELECT
   "PaymentIDBillerIDMerchantOrder",
    original_email,
    norm_email,
    SPLIT_PART(norm_email, '@', 1) AS username,
    SPLIT_PART(norm_email, '@', 2) AS domain
  FROM Base
),

EmailFeatures AS (
  SELECT
    rp."PaymentIDBillerIDMerchantOrder",
    rp.original_email,
    rp.norm_email,
    rp.username,
    rp.domain,

    -- presence / format
    CASE WHEN rp.norm_email = '' THEN 1 ELSE 0 END
      AS missing_email,
    CASE WHEN REGEXP_LIKE(rp.norm_email, '^[^@]+@[^@]+\\.[^@]+$') THEN 1 ELSE 0 END
      AS is_valid_format,

    -- business vs. free vs. disposable
    CASE WHEN REGEXP_LIKE(
           rp.domain,
           '\\b(COMPANY|CORP|INC|LLC|LLP|LTD|GMBH|PLC|ENTERPRISE|BUSINESS|CORPORATE|INDUSTRY|TECHNOLOGIES)\\b'
         ) THEN 1 ELSE 0 END                                            AS is_business_domain,
    CASE WHEN rp.domain LIKE '%gmail.com'   THEN 1 ELSE 0 END            AS is_gmail,
    CASE WHEN rp.domain LIKE '%yahoo.com'   THEN 1 ELSE 0 END            AS is_yahoo,
    CASE WHEN rp.domain LIKE '%hotmail.com' THEN 1 ELSE 0 END            AS is_hotmail,
    CASE WHEN rp.domain LIKE '%outlook.com' THEN 1 ELSE 0 END            AS is_outlook,
    CASE WHEN REGEXP_LIKE(
           rp.domain,
           'gmail\\.com|yahoo\\.com|hotmail\\.com|outlook\\.com|aol\\.com|protonmail\\.com|mail\\.com|icloud\\.com|zoho\\.com|yandex\\.com|gmx\\.com'
         ) THEN 1 ELSE 0 END                                            AS is_free_email,
    CASE WHEN REGEXP_LIKE(
           rp.domain,
           'mailinator\\.com|guerrillamail\\.com|temp-mail\\.org|10minutemail\\.com|throwawaymail\\.com|yopmail\\.com|getnada\\.com|dispostable\\.com|sharklasers\\.com|tempmail\\.net'
         ) THEN 1 ELSE 0 END                                            AS is_disposable_email,

    -- username analysis
    LENGTH(rp.username)                                               AS username_length,
    CASE WHEN rp.username = '' THEN 1 ELSE 0 END                       AS missing_username,
    CASE WHEN LENGTH(rp.username) < 6 THEN 1 ELSE 0 END                AS short_username,
    CASE WHEN LENGTH(rp.username) > 30 THEN 1 ELSE 0 END               AS long_username,
    CASE WHEN REGEXP_LIKE(rp.username, '[0-9]') THEN 1 ELSE 0 END       AS username_has_numbers,
    CASE WHEN REGEXP_LIKE(rp.username, '^[0-9]+$') THEN 1 ELSE 0 END    AS username_only_numbers,
    CASE WHEN REGEXP_LIKE(rp.norm_email, '\\b(test|fake|dummy|sample|example|demo)\\b') THEN 1 ELSE 0 END
                                                                      AS has_test_keyword,
    CASE WHEN REGEXP_LIKE(rp.username, '(123|abc|xyz|qwerty|asdf)') THEN 1 ELSE 0 END
                                                                      AS has_sequential_pattern,
    CASE WHEN REGEXP_LIKE(rp.username, '[^a-z0-9._-]') THEN 1 ELSE 0 END
                                                                      AS username_has_special_chars,
    CASE WHEN REGEXP_LIKE(rp.username, '^(admin|info|sales|support|contact|billing|finance|help|service|noreply)$') THEN 1 ELSE 0 END
                                                                      AS is_role_account,
    CASE WHEN REGEXP_LIKE(rp.username, '[0-9]')
          AND NOT REGEXP_LIKE(
                rp.domain,
                '\\b(COMPANY|CORP|INC|LLC|LLP|LTD|GMBH|PLC|ENTERPRISE|BUSINESS|CORPORATE|INDUSTRY|TECHNOLOGIES)\\b'
              )
         THEN 1 ELSE 0 END                                            AS has_numbers_not_business
  FROM RawParts rp
),

Scored AS (
  SELECT
    ef.*,
    COALESCE(ec.email_frequency, 0)                                 AS email_frequency,
    CASE WHEN COALESCE(ec.email_frequency, 0) > 5 THEN 1 ELSE 0 END  AS reused_email,
    (
      CASE WHEN ef.missing_email           = 1 THEN 10 ELSE  0 END +
      CASE WHEN ef.is_valid_format         = 0 AND ef.missing_email = 0 THEN  8 ELSE  0 END +
      CASE WHEN ef.is_disposable_email     = 1 THEN  9 ELSE  0 END +
      CASE WHEN ef.short_username          = 1 THEN  3 ELSE  0 END +
      CASE WHEN ef.username_only_numbers   = 1 THEN  4 ELSE  0 END +
      CASE WHEN ef.has_test_keyword        = 1 THEN  7 ELSE  0 END +
      CASE WHEN ef.has_sequential_pattern  = 1 THEN  4 ELSE  0 END +
      CASE WHEN ef.is_role_account         = 1 THEN  5 ELSE  0 END +
      CASE WHEN COALESCE(ec.email_frequency, 0) > 5 THEN  6 ELSE  0 END +
      CASE WHEN ef.has_numbers_not_business= 1 THEN  3 ELSE  0 END +
      CASE WHEN ef.username_has_special_chars = 1 THEN  2 ELSE  0 END
    )                                                               AS email_risk_score
  FROM EmailFeatures ef
  LEFT JOIN EmailCounts ec
    ON ef.norm_email = ec.email_key
)

SELECT
"PaymentIDBillerIDMerchantOrder",
  original_email       AS "EmailAddress",
  norm_email           AS email_clean,
  missing_email,
  is_valid_format,
  username,
  domain,
  is_business_domain,
  is_gmail,
  is_yahoo,
  is_hotmail,
  is_outlook,
  is_free_email,
  is_disposable_email,
  username_length,
  missing_username,
  short_username,
  long_username,
  username_has_numbers,
  username_only_numbers,
  has_test_keyword,
  has_sequential_pattern,
  username_has_special_chars,
  is_role_account,
  has_numbers_not_business,
  email_frequency,
  reused_email,
  email_risk_score,
  CASE
    WHEN email_risk_score <=  3 THEN 'Very Low'
    WHEN email_risk_score <=  7 THEN 'Low'
    WHEN email_risk_score <= 15 THEN 'Medium'
    ELSE                            'High'
  END AS email_risk_category
FROM Scored
ORDER BY email_risk_score DESC;
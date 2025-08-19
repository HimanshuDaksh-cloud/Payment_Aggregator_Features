WITH Name_Base AS (
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
  FROM Name_Base
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

  FROM Name_Base
),
Name_Scored AS (
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
,





-- Address
Address_Base AS (
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
  FROM Address_Base
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
  FROM Address_Base
),

Address_Scored AS (
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
),




-- Email Address
Email_Base AS (
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
  FROM Email_Base
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
  FROM Email_Base
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

Email_Scored AS (
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


-- IP Address
,
IPFeatures AS (
  SELECT 
    "PaymentIDBillerIDMerchantOrder",
    "RemoteIP" AS raw_ip,
    
    -- Basic cleaning
    TRIM(COALESCE("RemoteIP", '')) AS ip_clean,
    
    -- IP presence check
    CASE WHEN TRIM(COALESCE("RemoteIP", '')) = '' THEN 1 ELSE 0 END AS missing_ip,
    
    -- Basic IPv4 format validation
    REGEXP_LIKE(TRIM(COALESCE("RemoteIP", '')), 
                '^([0-9]{1,3}\\.){3}[0-9]{1,3}$') AS is_valid_ipv4_format,
    
    -- IPv6 format detection
    REGEXP_LIKE(TRIM(COALESCE("RemoteIP", '')), 
                ':') AS is_ipv6_format,
    
    -- Private IP address ranges (RFC1918) - considered "business" IPs
    REGEXP_LIKE(TRIM(COALESCE("RemoteIP", '')), 
                '^10\\.|^172\\.(1[6-9]|2[0-9]|3[0-1])\\.|^192\\.168\\.') AS is_private_ip,
    
    -- Business/organizational IP check (simplified)
    REGEXP_LIKE(TRIM(COALESCE("RemoteIP", '')), 
                '^(8\\.8\\.|13[0-9]\\.|14[4-9]\\.|15[0-9]\\.|16[0-9]\\.|17[0-2]\\.|19[2-9]\\.|20[0-9]\\.|21[0-4]\\.)') AS is_business_ip,
    
    -- Localhost detection
    REGEXP_LIKE(TRIM(COALESCE("RemoteIP", '')), 
                '^127\\.') AS is_localhost,
    
    -- Common test IPs
    REGEXP_LIKE(TRIM(COALESCE("RemoteIP", '')), 
                '^0\\.0\\.0\\.0$|^1\\.1\\.1\\.1$|^8\\.8\\.8\\.8$|^8\\.8\\.4\\.4$|^9\\.9\\.9\\.9$') AS is_test_ip,
    
    -- Extract all octets for more detailed analysis
    TRY_TO_NUMBER(SPLIT_PART(TRIM(COALESCE("RemoteIP", '')), '.', 1)) AS octet1,
    TRY_TO_NUMBER(SPLIT_PART(TRIM(COALESCE("RemoteIP", '')), '.', 2)) AS octet2,
    TRY_TO_NUMBER(SPLIT_PART(TRIM(COALESCE("RemoteIP", '')), '.', 3)) AS octet3,
    TRY_TO_NUMBER(SPLIT_PART(TRIM(COALESCE("RemoteIP", '')), '.', 4)) AS octet4,
    
    -- Check if IP contains suspicious number patterns but is not a business/private IP
    CASE 
      WHEN REGEXP_LIKE(TRIM(COALESCE("RemoteIP", '')), '(1234|2345|3456|4567|5678|6789|9876|8765|7654|6543|5432|4321|0000)') AND 
           NOT REGEXP_LIKE(TRIM(COALESCE("RemoteIP", '')), '^10\\.|^172\\.(1[6-9]|2[0-9]|3[0-1])\\.|^192\\.168\\.') AND
           NOT REGEXP_LIKE(TRIM(COALESCE("RemoteIP", '')), '^(8\\.8\\.|13[0-9]\\.|14[4-9]\\.|15[0-9]\\.|16[0-9]\\.|17[0-2]\\.|19[2-9]\\.|20[0-9]\\.|21[0-4]\\.)') 
      THEN 1 
      ELSE 0 
    END AS has_patterns_not_business
    
  FROM 
    CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
),

-- Count occurrences of each IP for frequency analysis
IPCounts AS (
  SELECT 
    TRIM(COALESCE("RemoteIP", '')) AS ip,
    COUNT(*) AS ip_frequency
  FROM 
    CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
  WHERE 
    TRIM(COALESCE("RemoteIP", '')) != ''
  GROUP BY 
    TRIM(COALESCE("RemoteIP", ''))
),

-- Add derived features
IPRiskFeatures AS (
  SELECT
    ip.*,
    
    -- Check if any octet is out of valid range (0-255)
    CASE WHEN (ip.octet1 < 0 OR ip.octet1 > 255 OR 
               ip.octet2 < 0 OR ip.octet2 > 255 OR 
               ip.octet3 < 0 OR ip.octet3 > 255 OR 
               ip.octet4 < 0 OR ip.octet4 > 255 OR
               ip.octet1 IS NULL OR ip.octet2 IS NULL OR 
               ip.octet3 IS NULL OR ip.octet4 IS NULL)
          AND ip.is_valid_ipv4_format = 1
         THEN 1 ELSE 0 
    END AS invalid_octet_range,
    
    -- Sequential pattern check (e.g., 1.2.3.4)
    CASE WHEN ip.octet2 = ip.octet1 + 1 AND 
              ip.octet3 = ip.octet2 + 1 AND 
              ip.octet4 = ip.octet3 + 1
         THEN 1 ELSE 0
    END AS sequential_octets,
    
    -- All octets same (e.g., 1.1.1.1)
    CASE WHEN ip.octet1 = ip.octet2 AND 
              ip.octet2 = ip.octet3 AND 
              ip.octet3 = ip.octet4 AND
              ip.octet1 IS NOT NULL
         THEN 1 ELSE 0
    END AS identical_octets,
    
    -- Repeated patterns (e.g., 1.1.2.2)
    CASE WHEN (ip.octet1 = ip.octet2 AND ip.octet3 = ip.octet4 AND
               ip.octet1 IS NOT NULL AND ip.octet3 IS NOT NULL)
         THEN 1 ELSE 0
    END AS paired_octets,
    
    -- IP Address Type Classification
    CASE 
        -- Type 1: Class A (1-126)
        WHEN ip.octet1 BETWEEN 1 AND 126 THEN 'Type 1'
        -- Type 2: Class B (128-191)
        WHEN ip.octet1 BETWEEN 128 AND 191 THEN 'Type 2'
        -- Type 3: Class C (192-223) - Small networks/offices 
        WHEN ip.octet1 BETWEEN 192 AND 223 THEN 'Type 3'
        -- Type 4: Class D (224-239) - Multicast
        WHEN ip.octet1 BETWEEN 224 AND 239 THEN 'Type 4'
        -- Type 5: Class E (240-255) - Experimental
        WHEN ip.octet1 BETWEEN 240 AND 255 THEN 'Type 5'
        -- Loopback address (127.x.x.x)
        WHEN ip.octet1 = 127 THEN 'Loopback'
        -- Invalid or missing
        ELSE 'Unknown/Invalid' 
    END AS ip_address_type
    
  FROM IPFeatures ip
)

,

IP_score as (

SELECT 
  ipf."PaymentIDBillerIDMerchantOrder",
  ipf.raw_ip,
  ipf.ip_clean,
  ipf.missing_ip,
  ipf.is_valid_ipv4_format,
  ipf.is_ipv6_format,
  ipf.is_private_ip,
  ipf.is_business_ip,
  ipf.is_localhost,
  ipf.is_test_ip,
  ipf.sequential_octets,
  ipf.identical_octets,
  ipf.paired_octets,
  ipf.has_patterns_not_business,
  ipf.ip_address_type,  -- IP address type classification
  
  -- Add type-specific risk flags
  CASE WHEN ipf.ip_address_type = 'Type 3' AND ipf.is_private_ip = 0 THEN 1 ELSE 0 END AS is_public_type3,
  CASE WHEN ipf.ip_address_type = 'Type 1' AND ipf.has_patterns_not_business = 1 THEN 1 ELSE 0 END AS suspicious_type1,
  CASE WHEN ipf.ip_address_type = 'Type 4' OR ipf.ip_address_type = 'Type 5' THEN 1 ELSE 0 END AS unusual_ip_class,
  
  -- IP frequency
  COALESCE(ipc.ip_frequency, 0) AS ip_frequency,
  CASE WHEN COALESCE(ipc.ip_frequency, 0) > 5 THEN 1 ELSE 0 END AS high_frequency_ip,
  
  -- IP risk score calculation
  (CASE WHEN ipf.missing_ip = 1 THEN 10 ELSE 0 END) +
  (CASE WHEN ipf.is_valid_ipv4_format = 0 AND ipf.is_ipv6_format = 0 AND ipf.missing_ip = 0 THEN 8 ELSE 0 END) +
  (CASE WHEN ipf.invalid_octet_range = 1 THEN 8 ELSE 0 END) +
  (CASE WHEN ipf.is_localhost = 1 THEN 9 ELSE 0 END) +
  -- Reduce score for business IPs with patterns
  (CASE WHEN ipf.sequential_octets = 1 AND ipf.is_business_ip = 0 THEN 7 
        WHEN ipf.sequential_octets = 1 AND ipf.is_business_ip = 1 THEN 2 ELSE 0 END) +
  (CASE WHEN ipf.identical_octets = 1 AND ipf.is_business_ip = 0 THEN 5
        WHEN ipf.identical_octets = 1 AND ipf.is_business_ip = 1 THEN 1 ELSE 0 END) +
  (CASE WHEN ipf.paired_octets = 1 AND ipf.is_business_ip = 0 THEN 3
        WHEN ipf.paired_octets = 1 AND ipf.is_business_ip = 1 THEN 1 ELSE 0 END) +
  -- Add IP type-specific risks
  (CASE WHEN ipf.ip_address_type = 'Type 4' OR ipf.ip_address_type = 'Type 5' THEN 6 ELSE 0 END) +
  (CASE WHEN ipf.ip_address_type = 'Type 3' AND ipf.is_private_ip = 0 THEN 2 ELSE 0 END) +
  (CASE WHEN ipf.ip_address_type = 'Unknown/Invalid' THEN 7 ELSE 0 END) +
  -- Other risk factors
  (CASE WHEN ipf.has_patterns_not_business = 1 THEN 3 ELSE 0 END) +
  (CASE WHEN COALESCE(ipc.ip_frequency, 0) > 10 THEN 5 ELSE 0 END)
  AS ip_risk_score,
  
  -- IP risk category - corrected syntax
  CASE 
    WHEN ((CASE WHEN ipf.missing_ip = 1 THEN 10 ELSE 0 END) +
          (CASE WHEN ipf.is_valid_ipv4_format = 0 AND ipf.is_ipv6_format = 0 AND ipf.missing_ip = 0 THEN 8 ELSE 0 END) +
          (CASE WHEN ipf.invalid_octet_range = 1 THEN 8 ELSE 0 END) +
          (CASE WHEN ipf.is_localhost = 1 THEN 9 ELSE 0 END) +
          (CASE WHEN ipf.sequential_octets = 1 AND ipf.is_business_ip = 0 THEN 7 
                WHEN ipf.sequential_octets = 1 AND ipf.is_business_ip = 1 THEN 2 ELSE 0 END) +
          (CASE WHEN ipf.identical_octets = 1 AND ipf.is_business_ip = 0 THEN 5
                WHEN ipf.identical_octets = 1 AND ipf.is_business_ip = 1 THEN 1 ELSE 0 END) +
          (CASE WHEN ipf.paired_octets = 1 AND ipf.is_business_ip = 0 THEN 3
                WHEN ipf.paired_octets = 1 AND ipf.is_business_ip = 1 THEN 1 ELSE 0 END) +
          (CASE WHEN ipf.has_patterns_not_business = 1 THEN 3 ELSE 0 END) +
          (CASE WHEN ipf.ip_address_type = 'Type 4' OR ipf.ip_address_type = 'Type 5' THEN 6 ELSE 0 END) +
          (CASE WHEN ipf.ip_address_type = 'Type 3' AND ipf.is_private_ip = 0 THEN 2 ELSE 0 END) +
          (CASE WHEN ipf.ip_address_type = 'Unknown/Invalid' THEN 7 ELSE 0 END) +
          (CASE WHEN COALESCE(ipc.ip_frequency, 0) > 10 THEN 5 ELSE 0 END)) <= 3 
    THEN 'Very Low'
    WHEN ((CASE WHEN ipf.missing_ip = 1 THEN 10 ELSE 0 END) +
          (CASE WHEN ipf.is_valid_ipv4_format = 0 AND ipf.is_ipv6_format = 0 AND ipf.missing_ip = 0 THEN 8 ELSE 0 END) +
          (CASE WHEN ipf.invalid_octet_range = 1 THEN 8 ELSE 0 END) +
          (CASE WHEN ipf.is_localhost = 1 THEN 9 ELSE 0 END) +
          (CASE WHEN ipf.sequential_octets = 1 AND ipf.is_business_ip = 0 THEN 7 
                WHEN ipf.sequential_octets = 1 AND ipf.is_business_ip = 1 THEN 2 ELSE 0 END) +
          (CASE WHEN ipf.identical_octets = 1 AND ipf.is_business_ip = 0 THEN 5
                WHEN ipf.identical_octets = 1 AND ipf.is_business_ip = 1 THEN 1 ELSE 0 END) +
          (CASE WHEN ipf.paired_octets = 1 AND ipf.is_business_ip = 0 THEN 3
                WHEN ipf.paired_octets = 1 AND ipf.is_business_ip = 1 THEN 1 ELSE 0 END) +
          (CASE WHEN ipf.has_patterns_not_business = 1 THEN 3 ELSE 0 END) +
          (CASE WHEN ipf.ip_address_type = 'Type 4' OR ipf.ip_address_type = 'Type 5' THEN 6 ELSE 0 END) +
          (CASE WHEN ipf.ip_address_type = 'Type 3' AND ipf.is_private_ip = 0 THEN 2 ELSE 0 END) +
          (CASE WHEN ipf.ip_address_type = 'Unknown/Invalid' THEN 7 ELSE 0 END) +
          (CASE WHEN COALESCE(ipc.ip_frequency, 0) > 10 THEN 5 ELSE 0 END)) <= 7 
    THEN 'Low'
    WHEN ((CASE WHEN ipf.missing_ip = 1 THEN 10 ELSE 0 END) +
          (CASE WHEN ipf.is_valid_ipv4_format = 0 AND ipf.is_ipv6_format = 0 AND ipf.missing_ip = 0 THEN 8 ELSE 0 END) +
          (CASE WHEN ipf.invalid_octet_range = 1 THEN 8 ELSE 0 END) +
          (CASE WHEN ipf.is_localhost = 1 THEN 9 ELSE 0 END) +
          (CASE WHEN ipf.sequential_octets = 1 AND ipf.is_business_ip = 0 THEN 7 
                WHEN ipf.sequential_octets = 1 AND ipf.is_business_ip = 1 THEN 2 ELSE 0 END) +
          (CASE WHEN ipf.identical_octets = 1 AND ipf.is_business_ip = 0 THEN 5
                WHEN ipf.identical_octets = 1 AND ipf.is_business_ip = 1 THEN 1 ELSE 0 END) +
          (CASE WHEN ipf.paired_octets = 1 AND ipf.is_business_ip = 0 THEN 3
                WHEN ipf.paired_octets = 1 AND ipf.is_business_ip = 1 THEN 1 ELSE 0 END) +
          (CASE WHEN ipf.has_patterns_not_business = 1 THEN 3 ELSE 0 END) +
          (CASE WHEN ipf.ip_address_type = 'Type 4' OR ipf.ip_address_type = 'Type 5' THEN 6 ELSE 0 END) +
          (CASE WHEN ipf.ip_address_type = 'Type 3' AND ipf.is_private_ip = 0 THEN 2 ELSE 0 END) +
          (CASE WHEN ipf.ip_address_type = 'Unknown/Invalid' THEN 7 ELSE 0 END) +
          (CASE WHEN COALESCE(ipc.ip_frequency, 0) > 10 THEN 5 ELSE 0 END)) <= 15 
    THEN 'Medium'
    ELSE 'High'
  END AS ip_risk_category,
  
  -- IP Classification
  CASE
    -- Invalid IPs
    WHEN ipf.missing_ip = 1 OR 
         (ipf.is_valid_ipv4_format = 0 AND ipf.is_ipv6_format = 0) OR
         ipf.invalid_octet_range = 1 OR
         ipf.is_localhost = 1
      THEN 'Invalid IP'
    
    -- Suspicious Non-Business IPs
    WHEN (ipf.sequential_octets = 1 OR ipf.identical_octets = 1 OR ipf.paired_octets = 1) AND
         ipf.is_business_ip = 0
      THEN 'Suspicious Pattern'
    
    -- Use IP class types
    ELSE ipf.ip_address_type
  END AS ip_classification

FROM 
  IPRiskFeatures ipf
LEFT JOIN 
  IPCounts ipc ON ipf.ip_clean = ipc.ip
)
,
-- Credit Card Number

CardFeatures AS (
  SELECT 
  "PaymentIDBillerIDMerchantOrder",
    "Cardnumber" AS full_card_number,
    
    -- Basic cleaning
    TRIM(COALESCE("Cardnumber", '')) AS card_clean,
    
    -- Card presence check
    CASE WHEN TRIM(COALESCE("Cardnumber", '')) = '' THEN 1 ELSE 0 END AS missing_card,
    
    -- Card length checks (after removing non-digits)
    LENGTH(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', '')) AS card_digits_length,
    
    -- Basic card format validation
    CASE WHEN REGEXP_LIKE(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), '^[0-9]+$') 
         THEN 1 ELSE 0 END AS is_numeric_only,
    
    -- First 6 digits (BIN/IIN) - Bank Identification Number
    LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 6) AS bin_number,
    
    -- Last 4 digits
    RIGHT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 4) AS last_four_digits,
    
    -- Card network detection
    CASE 
        WHEN LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 1) = '4' THEN 'Visa'
        WHEN LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 2) IN ('51', '52', '53', '54', '55') OR 
             LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 6) BETWEEN '222100' AND '272099' THEN 'Mastercard'
        WHEN LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 2) IN ('34', '37') THEN 'American Express'
        WHEN LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 2) IN ('36', '38', '39') OR
             LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 3) IN ('300', '301', '302', '303', '304', '305') THEN 'Diners Club'
        WHEN LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 4) = '6011' OR 
             LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 2) = '65' OR
             LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 3) BETWEEN '644' AND '649' THEN 'Discover'
        WHEN LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 2) IN ('35') THEN 'JCB'
        ELSE 'Unknown'
    END AS card_network,
    
    -- Business card detection (equivalent to is_business_entity in your function)
    CASE 
        WHEN LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 6) IN 
             ('485913', '485924', '485916', '485923', '486229', '486494', '486496', '490806', -- Visa Business 
              '552356', '552455', '552456', '552503', '552571', '552721', '557169', '557200', -- Mastercard Business
              '370728', '370729', '370710', '370711', '370712', '370713', '370714', '370715') -- Amex Business
             OR LEFT(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 4) IN 
             ('3770', '3771', '3772', '3773', '3774', '3775', '3776', '3777') -- More Amex Business
        THEN 1 ELSE 0 
    END AS is_business_card,
    
    -- Test/fake card detection (common test numbers)
    CASE WHEN REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', '') IN (
              '4111111111111111', '4242424242424242', '5555555555554444', 
              '378282246310005', '371449635398431', '6011111111111117', 
              '5105105105105100', '30569309025904', '38520000023237')
         THEN 1 ELSE 0 END AS is_test_card,
         
    -- Check for patterns that may indicate test/fake cards
    CASE 
        WHEN REGEXP_LIKE(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 
                         '0000|1111|2222|3333|4444|5555|6666|7777|8888|9999') THEN 1 
        ELSE 0 
    END AS has_repeated_digits,
    
    -- Sequential number patterns check
    CASE 
        WHEN REGEXP_LIKE(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 
                         '1234|2345|3456|4567|5678|6789|9876|8765|7654|6543|5432|4321') THEN 1 
        ELSE 0 
    END AS has_sequential_digits,
    
    -- Check for keyboard patterns (like in your function)
    CASE 
        WHEN REGEXP_LIKE(REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', ''), 
                         'qwer|asdf|zxcv|wasd') THEN 1 
        ELSE 0 
    END AS has_keyboard_pattern
    
  FROM 
    CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
),

-- Count occurrences of each card number (similar to name_frequency)
CardCounts AS (
  SELECT 
    REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', '') AS card,
    COUNT(*) AS card_frequency
  FROM 
    CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
  WHERE 
    TRIM(COALESCE("Cardnumber", '')) != ''
  GROUP BY 
    REGEXP_REPLACE(TRIM(COALESCE("Cardnumber", '')), '[^0-9]', '')
),

-- Derived features with has_numbers_not_business logic
CardRiskFeatures AS (
  SELECT
    cf.*,
    
    -- Invalid card length by type
    CASE 
      WHEN cf.card_network = 'Visa' AND cf.card_digits_length NOT IN (13, 16, 19) THEN 1
      WHEN cf.card_network = 'Mastercard' AND cf.card_digits_length != 16 THEN 1
      WHEN cf.card_network = 'American Express' AND cf.card_digits_length != 15 THEN 1
      WHEN cf.card_network = 'Discover' AND cf.card_digits_length != 16 THEN 1
      WHEN cf.card_network = 'JCB' AND cf.card_digits_length NOT IN (15, 16) THEN 1
      WHEN cf.card_network = 'Diners Club' AND cf.card_digits_length NOT IN (14, 16) THEN 1
      ELSE 0
    END AS invalid_length_for_network,
    
    -- Direct implementation of has_numbers_not_business logic for cards
    -- Checks for suspicious patterns in non-business cards only
    CASE 
      WHEN (cf.has_repeated_digits = 1 OR cf.has_sequential_digits = 1) AND cf.is_business_card = 0 THEN 1
      ELSE 0 
    END AS has_patterns_not_business,
    
    -- Single digit card check (extreme case)
    CASE 
      WHEN REGEXP_LIKE(REGEXP_REPLACE(TRIM(COALESCE(cf.full_card_number, '')), '[^0-9]', ''), 
                      '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$') THEN 1
      ELSE 0
    END AS single_digit_card
    
  FROM CardFeatures cf
)
,
card_score as (
  crf."PaymentIDBillerIDMerchantOrder",
  crf.full_card_number,
  -- For security, mask all but last 4 digits
  CASE 
    WHEN crf.card_digits_length > 4 THEN 
      CONCAT(REPEAT('*', crf.card_digits_length - 4), crf.last_four_digits)
    ELSE crf.card_clean
  END AS masked_card_number,
  
  crf.missing_card,
  crf.card_digits_length,
  crf.is_numeric_only,
  crf.card_network,
  crf.bin_number,
  crf.is_business_card,           -- Equivalent to is_business_entity
  crf.is_test_card,               -- Equivalent to is_test_name
  crf.has_repeated_digits,        -- Equivalent to has_repeated_chars
  crf.has_sequential_digits,      -- Similar to has_keyboard_pattern
  crf.has_keyboard_pattern,       -- Direct equivalent to your function
  crf.invalid_length_for_network, -- Card-specific validation
  crf.has_patterns_not_business,  -- Direct equivalent to has_numbers_not_business
  crf.single_digit_card,          -- Extreme repetition case
  
  -- Card frequency (similar to name_frequency)
  COALESCE(cc.card_frequency, 0) AS card_frequency,
  CASE WHEN COALESCE(cc.card_frequency, 0) > 1 THEN 1 ELSE 0 END AS multiple_transactions_same_card,
  
  -- Credit card risk score calculation - follows same pattern as your name_risk_score
  (
    -- Reduce scores for business cards (same approach as your code)
    (crf.has_repeated_digits * CASE WHEN crf.is_business_card = 1 THEN 1 ELSE 8 END) +
    (crf.has_sequential_digits * CASE WHEN crf.is_business_card = 1 THEN 1 ELSE 8 END) +
    
    -- Card-specific scores
    (crf.missing_card * 10) +
    (CASE WHEN crf.is_numeric_only = 0 AND crf.missing_card = 0 THEN 10 ELSE 0 END) +
    (crf.invalid_length_for_network * 9) +
    
    -- Regular scores (using same weights as your code where possible)
    (crf.is_test_card * 10) +                                 -- Same as is_test_name (10)
    (crf.single_digit_card * 10) +                            -- Extreme case
    (crf.has_patterns_not_business * 3) +                     -- Same as has_numbers (3)
    (crf.has_keyboard_pattern * 8) +                          -- Same as has_keyboard_pattern (8)
    (CASE WHEN crf.card_network = 'Unknown' THEN 7 ELSE 0 END) + -- Similar to contains_test_keyword (7)
    (CASE WHEN COALESCE(cc.card_frequency, 0) > 10 THEN 6 ELSE 0 END) -- Frequency check
  ) AS card_risk_score,
  
  -- Card risk category (using same thresholds as your scoring would use)
  CASE 
    WHEN (
      (crf.has_repeated_digits * CASE WHEN crf.is_business_card = 1 THEN 1 ELSE 8 END) +
      (crf.has_sequential_digits * CASE WHEN crf.is_business_card = 1 THEN 1 ELSE 8 END) +
      (crf.missing_card * 10) +
      (CASE WHEN crf.is_numeric_only = 0 AND crf.missing_card = 0 THEN 10 ELSE 0 END) +
      (crf.invalid_length_for_network * 9) +
      (crf.is_test_card * 10) +
      (crf.single_digit_card * 10) +
      (crf.has_patterns_not_business * 3) +
      (crf.has_keyboard_pattern * 8) +
      (CASE WHEN crf.card_network = 'Unknown' THEN 7 ELSE 0 END) +
      (CASE WHEN COALESCE(cc.card_frequency, 0) > 10 THEN 6 ELSE 0 END)
    ) <= 3 THEN 'Very Low'
    WHEN (
      (crf.has_repeated_digits * CASE WHEN crf.is_business_card = 1 THEN 1 ELSE 8 END) +
      (crf.has_sequential_digits * CASE WHEN crf.is_business_card = 1 THEN 1 ELSE 8 END) +
      (crf.missing_card * 10) +
      (CASE WHEN crf.is_numeric_only = 0 AND crf.missing_card = 0 THEN 10 ELSE 0 END) +
      (crf.invalid_length_for_network * 9) +
      (crf.is_test_card * 10) +
      (crf.single_digit_card * 10) +
      (crf.has_patterns_not_business * 3) +
      (crf.has_keyboard_pattern * 8) +
      (CASE WHEN crf.card_network = 'Unknown' THEN 7 ELSE 0 END) +
      (CASE WHEN COALESCE(cc.card_frequency, 0) > 10 THEN 6 ELSE 0 END)
    ) <= 7 THEN 'Low'
    WHEN (
      (crf.has_repeated_digits * CASE WHEN crf.is_business_card = 1 THEN 1 ELSE 8 END) +
      (crf.has_sequential_digits * CASE WHEN crf.is_business_card = 1 THEN 1 ELSE 8 END) +
      (crf.missing_card * 10) +
      (CASE WHEN crf.is_numeric_only = 0 AND crf.missing_card = 0 THEN 10 ELSE 0 END) +
      (crf.invalid_length_for_network * 9) +
      (crf.is_test_card * 10) +
      (crf.single_digit_card * 10) +
      (crf.has_patterns_not_business * 3) +
      (crf.has_keyboard_pattern * 8) +
      (CASE WHEN crf.card_network = 'Unknown' THEN 7 ELSE 0 END) +
      (CASE WHEN COALESCE(cc.card_frequency, 0) > 10 THEN 6 ELSE 0 END)
    ) <= 15 THEN 'Medium'
    ELSE 'High'
  END AS card_risk_category,
  
  -- Card Type Classification (equivalent to your address type bifurcation)
  CASE
    -- Type 1: Invalid Cards (highest risk)
    WHEN crf.missing_card = 1 OR 
         crf.is_numeric_only = 0 OR
         crf.invalid_length_for_network = 1 OR
         crf.single_digit_card = 1
      THEN 'Type 1'
    
    -- Type 2: Test/Suspicious Consumer Cards (high risk)
    WHEN crf.is_test_card = 1 OR
         crf.has_patterns_not_business = 1
      THEN 'Type 2'
    
    -- Type 3: Business Cards with Patterns (medium risk)
    WHEN (crf.has_repeated_digits = 1 OR crf.has_sequential_digits = 1) AND 
         crf.is_business_card = 1
      THEN 'Type 3'
    
    -- Type 4: Normal Cards (low risk)
    ELSE 'Type 4'
  END AS card_type

FROM 
  CardRiskFeatures crf
LEFT JOIN 
  CardCounts cc ON REGEXP_REPLACE(crf.card_clean, '[^0-9]', '') = cc.card
  ),

-- Payment Source
Payment_Cleaned AS (
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

  FROM Payment_Cleaned
),

ChannelCounts AS (
  SELECT
    channel_normalized AS channel,
    COUNT(*)            AS channel_frequency,
    COUNT(DISTINCT CUSTOMER_ID) AS distinct_customers
  FROM Payment_Cleaned
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

,

Payment_score as (

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

FROM ChannelRiskFeatures crf),
 

-- MCC Description

MCCFeatures AS (
  SELECT 

     "PaymentIDBillerIDMerchantOrder",
    "MCC_DESCRIPTION" AS raw_mcc,  -- Correct column name
    
    -- Basic cleaning
    TRIM(COALESCE("MCC_DESCRIPTION", '')) AS mcc_clean,
    
    -- MCC presence check
    CASE WHEN TRIM(COALESCE("MCC_DESCRIPTION", '')) = '' THEN 1 ELSE 0 END AS missing_mcc,
    
    -- Length checks
    LENGTH(TRIM(COALESCE("MCC_DESCRIPTION", ''))) AS mcc_length,
    
    -- Normalize MCC names for comparison
    UPPER(TRIM(COALESCE("MCC_DESCRIPTION", ''))) AS mcc_normalized,
    
    -- Risk classification of MCC categories
    -- Higher risk categories get value 1, lower risk categories get 0
    CASE WHEN UPPER(TRIM(COALESCE("MCC_DESCRIPTION", ''))) IN (
      'INSURANCE SALES, UNDERWRITING, AND PREMIUMS',
      'POLITICAL ORGANIZATIONS',
      'COURT COSTS, INCLUDING ALIMONY AND CHILD SUPPORT',
      'PROTECTIVE AND SECURITY SERVICES – INCLUDING ARMORED CARSAND GUARD DOGS',
      'CHARITABLE AND SOCIAL SERVICE ORGANIZATIONS',
      'LEGAL SERVICES AND ATTORNEYS',
      'FINANCIAL INSTITUTIONS – MERCHANDISE AND SERVICES',
      'FINES',
      'GOVERNMENT SERVICES ( NOT ELSEWHERE CLASSIFIED)',
      'TAX PAYMENTS'
    ) THEN 1 ELSE 0 END AS is_high_risk_mcc,
    
    -- Low-risk, standard categories 
    CASE WHEN UPPER(TRIM(COALESCE("MCC_DESCRIPTION", ''))) IN (
      'SCHOOLS AND EDUCATIONAL SERVICES ( NOT ELSEWHERE CLASSIFIED)',
      'TESTING LABORATORIES ( NON-MEDICAL)',
      'FAX SERVICES, TELECOMMUNICATION SERVICES',
      'CABLE AND OTHER PAY TELEVISION (PREVIOUSLY CABLE SERVICES)',
      'COMPUTER MAINTENANCE AND REPAIR SERVICES, NOT ELSEWHERE CLASSIFIED',
      'BUSINESS SERVICES, NOT ELSEWHERE CLASSIFIED',
      'MEMBERSHIP ORGANIZATIONS ( NOT ELSEWHERE CLASSIFIED)',
      'AUTOMOBILE PARKING LOTS AND GARAGES',
      'ELECTRIC, GAS, SANITARY AND WATER UTILITIES',
      'ARCHITECTURAL – ENGINEERING AND SURVEYING SERVICES',
      'CARD SHOPS, GIFT, NOVELTY, AND SOUVENIR SHOPS',
      'MEMBERSHIP CLUBS (SPORTS, RECREATION, ATHLETIC), COUNTRY CLUBS, AND PRIVATE GOLF COURSES',
      'RECREATION SERVICES (NOT ELSEWHERE CLASSIFIED)'
    ) THEN 1 ELSE 0 END AS is_standard_mcc,
    
    -- Some MCC categories that may appear suspicious but are legitimate for business context
    CASE WHEN UPPER(TRIM(COALESCE("MCC_DESCRIPTION", ''))) IN (
      'FINANCIAL INSTITUTIONS – MERCHANDISE AND SERVICES',
      'LEGAL SERVICES AND ATTORNEYS',
      'INSURANCE SALES, UNDERWRITING, AND PREMIUMS',
      'BUSINESS SERVICES, NOT ELSEWHERE CLASSIFIED'
    ) THEN 1 ELSE 0 END AS is_business_related_mcc,
    
    -- Government/Public sector related MCCs
    CASE WHEN UPPER(TRIM(COALESCE("MCC_DESCRIPTION", ''))) IN (
      'GOVERNMENT SERVICES ( NOT ELSEWHERE CLASSIFIED)',
      'TAX PAYMENTS',
      'FINES',
      'COURT COSTS, INCLUDING ALIMONY AND CHILD SUPPORT',
      'POLITICAL ORGANIZATIONS'
    ) THEN 1 ELSE 0 END AS is_government_related,
    
    -- Check for Not Elsewhere Classified (NEC) categories
    CASE WHEN UPPER(TRIM(COALESCE("MCC_DESCRIPTION", ''))) LIKE '%NOT ELSEWHERE CLASSIFIED%' 
         OR UPPER(TRIM(COALESCE("MCC_DESCRIPTION", ''))) LIKE '%NEC%' 
         THEN 1 ELSE 0 END AS is_nec_category
    
  FROM 
    CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
),

-- Count occurrences of each MCC for frequency analysis
MCCCounts AS (
  SELECT 
    UPPER(TRIM(COALESCE("MCC_DESCRIPTION", ''))) AS mcc,
    COUNT(*) AS mcc_frequency,
    COUNT(DISTINCT "CustomerID") AS distinct_customers, -- Adjust column name if different
    SUM(COALESCE("PaymentAmount", 0)) AS total_amount -- Adjust column name if different
  FROM 
    CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
  WHERE 
    TRIM(COALESCE("MCC_DESCRIPTION", '')) != ''
  GROUP BY 
    UPPER(TRIM(COALESCE("MCC_DESCRIPTION", '')))
),

-- Add derived risk features
MCCRiskFeatures AS (
  SELECT
    mf.*,
    
    -- Non-standard MCC descriptions
    CASE WHEN mf.is_standard_mcc = 0 AND mf.missing_mcc = 0 THEN 1 ELSE 0 END AS is_unusual_mcc,
    
    -- All caps check
    CASE WHEN mf.mcc_clean = UPPER(mf.mcc_clean) AND mf.mcc_clean != '' THEN 1 ELSE 0 END AS is_all_caps,
    
    -- Numbers in MCC - contextual approach like has_numbers_not_business
    -- Numbers are suspicious in standard MCC descriptions but might be ok in business-related ones
    CASE 
      WHEN REGEXP_LIKE(mf.mcc_clean, '[0-9]') AND mf.is_business_related_mcc = 0 THEN 1 
      ELSE 0 
    END AS has_numbers_not_business,
    
    -- Special characters beyond what's expected in standard MCC descriptions
    CASE WHEN REGEXP_LIKE(mf.mcc_clean, '[^a-zA-Z0-9\\s\\-\\_\\(\\)\\.,]') THEN 1 ELSE 0 END AS has_special_chars,
    
    -- Check for shortened MCCs (possibly suspicious abbreviations)
    CASE WHEN LENGTH(mf.mcc_clean) < 10 AND mf.missing_mcc = 0 THEN 1 ELSE 0 END AS mcc_too_short,
    
    -- Very long MCCs (possibly concatenated or data issues)
    CASE WHEN LENGTH(mf.mcc_clean) > 100 THEN 1 ELSE 0 END AS mcc_too_long,
    
    -- Check for test keywords
    CASE WHEN REGEXP_LIKE(UPPER(mf.mcc_clean), 'TEST|DUMMY|SAMPLE|DEMO|DEV') THEN 1 ELSE 0 END AS is_test_mcc
    
  FROM MCCFeatures mf
)

-- Partner
,

Partner_Features as(
SELECT 
    "PaymentIDBillerIDMerchantOrder",
    "Partner",
    
    -- Partner categorization
    CASE
        -- Enterprise partners (lower risk)
        WHEN "Partner" LIKE '%Oracle%' OR "Partner" LIKE '%SAP%' 
          OR "Partner" LIKE '%Guidewire%' OR "Partner" LIKE '%Tyler%'
          OR "Partner" LIKE '%Harris%' OR "Partner" LIKE '%Duck Creek%'
          OR "Partner" LIKE '%Sapiens%' OR "Partner" LIKE '%SunGard%'
          OR "Partner" LIKE '%Hansen%' THEN 'Enterprise'
        
        -- Mid-tier partners (medium risk)
        WHEN "Partner" LIKE '%Central Square%' OR "Partner" LIKE '%NaviLine%'
          OR "Partner" LIKE '%CityWorks%' OR "Partner" LIKE '%Civic Systems%'
          OR "Partner" LIKE '%Caselle%' OR "Partner" LIKE '%Software Solutions%'
          OR "Partner" LIKE '%IMT%' OR "Partner" LIKE '%Cogsdale%'
          OR "Partner" LIKE '%Systems and Software%' OR "Partner" LIKE '%Muni-Link%'
          OR "Partner" LIKE '%OpenGov%' THEN 'Mid-tier'
        
        -- Custom/in-house solutions (higher risk)
        WHEN "Partner" LIKE '%In-House%' OR "Partner" LIKE '%Home Grown%'
          OR "Partner" LIKE '%Custom%' THEN 'Custom'
          
        ELSE 'Other'
    END AS "partner_category",
    
    -- Industry type
    CASE
        WHEN "Partner" LIKE '%Munis%' OR "Partner" LIKE '%Municipal%' 
          OR "Partner" LIKE '%Govern%' THEN 'Municipal'
        WHEN "Partner" LIKE '%Insurance%' OR "Partner" LIKE '%Duck Creek%'
          OR "Partner" LIKE '%Sapiens%' OR "Partner" LIKE '%Insurity%' THEN 'Insurance'
        WHEN "Partner" LIKE '%Util%' OR "Partner" LIKE '%Water%' 
          OR "Partner" LIKE '%Waste%' THEN 'Utility'
        ELSE 'Other'
    END AS "partner_industry",
    
    -- Basic quality flags
    CASE WHEN "Partner" IS NULL OR TRIM("Partner") = '' THEN 1 ELSE 0 END AS "partner_missing",
    CASE WHEN LENGTH("Partner") < 3 THEN 1 ELSE 0 END AS "partner_name_too_short",
    
    -- Test system indicators
    CASE WHEN "Partner" LIKE '%Test%' OR "Partner" LIKE '%Demo%' 
         OR "Partner" LIKE '%Sample%' OR "Partner" LIKE '%Dummy%' THEN 1 ELSE 0 END AS "is_test_partner",
    
    -- System age indicators
    CASE WHEN "Partner" LIKE '%AS400%' OR "Partner" LIKE '%Iseries%'
         OR "Partner" LIKE '%Mainframe%' OR "Partner" LIKE '%LOCiS%' THEN 1 ELSE 0 END AS "legacy_system",
    
    CASE WHEN "Partner" LIKE '%Cloud%' OR "Partner" LIKE '%Native%'
         OR "Partner" LIKE '%Real-Time%' OR "Partner" LIKE '%Real Time%' THEN 1 ELSE 0 END AS "modern_system",
    
    -- Integration pattern indicators
    CASE WHEN "Partner" LIKE '%Batch%' THEN 1 ELSE 0 END AS "batch_system",
    CASE WHEN "Partner" LIKE '%Real-Time%' OR "Partner" LIKE '%Real Time%' THEN 1 ELSE 0 END AS "real_time_system",
    
    -- Risk flags based on partner types
    CASE WHEN "Partner" LIKE '%Oracle%' OR "Partner" LIKE '%SAP%' 
         OR "Partner" LIKE '%Guidewire%' OR "Partner" LIKE '%Tyler%'
         OR "Partner" LIKE '%Harris%' OR "Partner" LIKE '%Duck Creek%'
         OR "Partner" LIKE '%Sapiens%' OR "Partner" LIKE '%SunGard%'
         OR "Partner" LIKE '%Hansen%' THEN 1 ELSE 0 END AS "is_enterprise_partner",
         
    CASE WHEN "Partner" LIKE '%Central Square%' OR "Partner" LIKE '%NaviLine%'
         OR "Partner" LIKE '%CityWorks%' OR "Partner" LIKE '%Civic Systems%'
         OR "Partner" LIKE '%Caselle%' OR "Partner" LIKE '%Software Solutions%'
         OR "Partner" LIKE '%IMT%' OR "Partner" LIKE '%Cogsdale%'
         OR "Partner" LIKE '%Systems and Software%' OR "Partner" LIKE '%Muni-Link%'
         OR "Partner" LIKE '%OpenGov%' THEN 1 ELSE 0 END AS "is_mid_tier_partner",
         
    CASE WHEN "Partner" LIKE '%In-House%' OR "Partner" LIKE '%Home Grown%'
         OR "Partner" LIKE '%Custom%' THEN 1 ELSE 0 END AS "is_custom_solution",
    
    -- Partner frequency metrics
    (SELECT COUNT(*) FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3" t2 
     WHERE t2."Partner" = t1."Partner") AS "partner_transaction_count",
     
    CASE WHEN (SELECT COUNT(*) FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3" t2 
              WHERE t2."Partner" = t1."Partner") < 10 THEN 1 ELSE 0 END AS "low_volume_partner",
              
    -- Partner diversity metrics (how many different partners used this payment ID)
    (SELECT COUNT(DISTINCT t2."Partner") FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3" t2 
     WHERE t2."PaymentIDBillerIDMerchantOrder" = t1."PaymentIDBillerIDMerchantOrder") AS "distinct_partners_per_id",
     
    CASE WHEN (SELECT COUNT(DISTINCT t2."Partner") FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3" t2 
              WHERE t2."PaymentIDBillerIDMerchantOrder" = t1."PaymentIDBillerIDMerchantOrder") > 1 
         THEN 1 ELSE 0 END AS "multiple_partners_same_id",
    
    -- Calculate composite partner risk score
    (
        -- Base score from partner category
        CASE
            WHEN "Partner" LIKE '%Oracle%' OR "Partner" LIKE '%SAP%' 
              OR "Partner" LIKE '%Guidewire%' OR "Partner" LIKE '%Tyler%'
              OR "Partner" LIKE '%Harris%' OR "Partner" LIKE '%Duck Creek%'
              OR "Partner" LIKE '%Sapiens%' OR "Partner" LIKE '%SunGard%'
              OR "Partner" LIKE '%Hansen%' THEN 2  -- Enterprise (low risk)
            WHEN "Partner" LIKE '%Central Square%' OR "Partner" LIKE '%NaviLine%'
              OR "Partner" LIKE '%CityWorks%' OR "Partner" LIKE '%Civic Systems%'
              OR "Partner" LIKE '%Caselle%' OR "Partner" LIKE '%Software Solutions%'
              OR "Partner" LIKE '%IMT%' OR "Partner" LIKE '%Cogsdale%'
              OR "Partner" LIKE '%Systems and Software%' OR "Partner" LIKE '%Muni-Link%'
              OR "Partner" LIKE '%OpenGov%' THEN 5  -- Mid-tier (medium risk)
            WHEN "Partner" LIKE '%In-House%' OR "Partner" LIKE '%Home Grown%'
              OR "Partner" LIKE '%Custom%' THEN 8  -- Custom (high risk)
            ELSE 6  -- Other (default moderate-high risk)
        END +
        
        -- Risk modifiers
        CASE WHEN "Partner" IS NULL OR TRIM("Partner") = '' THEN 10 ELSE 0 END +
        CASE WHEN LENGTH("Partner") < 3 THEN 5 ELSE 0 END +
        CASE WHEN "Partner" LIKE '%Test%' OR "Partner" LIKE '%Demo%' THEN 10 ELSE 0 END +
        CASE WHEN "Partner" LIKE '%AS400%' OR "Partner" LIKE '%Iseries%' OR "Partner" LIKE '%Mainframe%' OR "Partner" LIKE '%LOCiS%' THEN 4 ELSE 0 END -
        CASE WHEN "Partner" LIKE '%Cloud%' OR "Partner" LIKE '%Native%' OR "Partner" LIKE '%Real-Time%' OR "Partner" LIKE '%Real Time%' THEN 2 ELSE 0 END +
        CASE WHEN (SELECT COUNT(*) FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3" t2 WHERE t2."Partner" = t1."Partner") < 10 THEN 3 ELSE 0 END +
        CASE WHEN (SELECT COUNT(DISTINCT t2."Partner") FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3" t2 WHERE t2."PaymentIDBillerIDMerchantOrder" = t1."PaymentIDBillerIDMerchantOrder") > 1 THEN 6 ELSE 0 END
    ) AS "partner_risk_score"

FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3" t1),

-- Query to extract risk features using RUCA codes with first 5 digits of ZIP code
Geo_Features as (
SELECT 
    t1."PaymentIDBillerIDMerchantOrder",
    t1."Zip" AS "original_zip",
    
    -- Extract first 5 digits of cleaned ZIP codes
    LEFT(TRIM(REGEXP_REPLACE(t1."Zip", '[^A-Za-z0-9]', '')), 5) AS "zip_5digit",
    LEFT(TRIM(REGEXP_REPLACE(ruca."ZIP_CODE", '[^A-Za-z0-9]', '')), 5) AS "ruca_zip_5digit",
    
    t1."BillingLatitude", 
    t1."BillingLongitude",
    t1."State" AS "transaction_state",
    
    -- RUCA data
    ruca."ZIP_CODE" AS "original_ruca_zip",
    ruca."RUCA1",
    ruca."RUCA2",
    ruca."STATE" AS "zip_state",
    
    -- Rural/Urban classification based on RUCA1 code
    CASE 
        WHEN ruca."RUCA1" IS NULL THEN 'Unknown'
        WHEN ruca."RUCA1" = 1 THEN 'Metropolitan Core (Urban)'
        WHEN ruca."RUCA1" BETWEEN 2 AND 3 THEN 'Metropolitan Commuting (Suburban)'
        WHEN ruca."RUCA1" BETWEEN 4 AND 6 THEN 'Micropolitan'
        WHEN ruca."RUCA1" BETWEEN 7 AND 9 THEN 'Small Town'
        WHEN ruca."RUCA1" = 10 THEN 'Rural'
        ELSE 'Unknown'
    END AS "area_type",
    
    -- Create binary flags for each area type
    CASE WHEN ruca."RUCA1" = 1 THEN 1 ELSE 0 END AS "is_metropolitan_core",
    CASE WHEN ruca."RUCA1" BETWEEN 2 AND 3 THEN 1 ELSE 0 END AS "is_metro_commuting",
    CASE WHEN ruca."RUCA1" BETWEEN 4 AND 6 THEN 1 ELSE 0 END AS "is_micropolitan",
    CASE WHEN ruca."RUCA1" BETWEEN 7 AND 9 THEN 1 ELSE 0 END AS "is_small_town",
    CASE WHEN ruca."RUCA1" = 10 THEN 1 ELSE 0 END AS "is_rural",
    
    -- Geographic risk factors
    -- Rural areas with high payment amounts may indicate higher risk
    CASE WHEN ruca."RUCA1" = 10 AND t1."PaymentAmount" > 1000 THEN 1 ELSE 0 END AS "high_value_rural_transaction",
    
    -- State-ZIP mismatch
    CASE WHEN t1."State" IS NOT NULL AND ruca."STATE" IS NOT NULL AND 
              t1."State" <> ruca."STATE" THEN 1 ELSE 0 END AS "state_zip_mismatch",
    
    -- Calculate geographic risk score
    (
        -- Missing/invalid values
        (CASE WHEN t1."BillingLatitude" IS NULL OR t1."BillingLongitude" IS NULL THEN 8 ELSE 0 END) +
        (CASE WHEN t1."Zip" IS NULL OR TRIM(t1."Zip") = '' THEN 7 ELSE 0 END) +
        
        -- Format validation
        (CASE WHEN t1."BillingLatitude" < -90 OR t1."BillingLatitude" > 90 OR
                t1."BillingLongitude" < -180 OR t1."BillingLongitude" > 180 THEN 9 ELSE 0 END) +
        
        -- Default/test location detection
        (CASE WHEN (t1."BillingLatitude" = 0 AND t1."BillingLongitude" = 0) OR
                  (t1."BillingLatitude" BETWEEN -0.001 AND 0.001 AND t1."BillingLongitude" BETWEEN -0.001 AND 0.001)
             THEN 10 ELSE 0 END) +
        (CASE WHEN t1."Zip" IN ('00000', '99999', '12345', '54321', '90210') THEN 7 ELSE 0 END) +
        
        -- RUCA-based risk factors
        (CASE WHEN ruca."RUCA1" IS NULL THEN 4  -- Unknown location (higher risk)
              WHEN ruca."RUCA1" = 10 THEN 3     -- Rural (higher risk)
              WHEN ruca."RUCA1" BETWEEN 7 AND 9 THEN 2  -- Small town (moderate risk)
              WHEN ruca."RUCA1" BETWEEN 4 AND 6 THEN 0  -- Micropolitan (neutral risk)
              WHEN ruca."RUCA1" BETWEEN 2 AND 3 THEN -1 -- Metro commuting (slightly lower risk)
              WHEN ruca."RUCA1" = 1 THEN -2     -- Metropolitan core (lowest risk)
              ELSE 4  -- Default to higher risk if we can't determine
         END) +
         
        -- Transaction context
        (CASE WHEN ruca."RUCA1" = 10 AND t1."PaymentAmount" > 1000 THEN 5 ELSE 0 END) +
        (CASE WHEN t1."State" IS NOT NULL AND ruca."STATE" IS NOT NULL AND 
                  t1."State" <> ruca."STATE" THEN 6 ELSE 0 END)
    ) AS "geo_risk_score"
    
FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3" t1
LEFT JOIN CONSOLIDATED_METRICS.SCRATCH."US_ZIP_RURAL_URBAN_MAPPING" ruca 
    -- Join on first 5 digits of cleaned ZIP codes
    ON LEFT(TRIM(REGEXP_REPLACE(t1."Zip", '[^A-Za-z0-9]', '')), 5) = 
       LEFT(TRIM(REGEXP_REPLACE(ruca."ZIP_CODE", '[^A-Za-z0-9]', '')), 5)
WHERE t1."Zip" IS NOT NULL  -- Exclude records without ZIP codes)
),



-- EBPP Features

EBPPFeatures AS (
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
),

-- Pricing Model
Pricing_Features as (SELECT
  "PaymentIDBillerIDMerchantOrder",
  "Pricing Model" AS original_pricing_model,
  
  -- One-hot encoding for Pricing Model
  CASE WHEN UPPER(TRIM(COALESCE("Pricing Model", ''))) = 'ABSORB' THEN 1 ELSE 0 END AS pricing_model_absorb,
  CASE WHEN UPPER(TRIM(COALESCE("Pricing Model", ''))) = 'HYBRID' THEN 1 ELSE 0 END AS pricing_model_hybrid,
  CASE WHEN UPPER(TRIM(COALESCE("Pricing Model", ''))) = 'SUBMIT' THEN 1 ELSE 0 END AS pricing_model_submit,
  CASE WHEN UPPER(TRIM(COALESCE("Pricing Model", ''))) = 'NO PRICING' THEN 1 ELSE 0 END AS pricing_model_no_pricing,
  CASE WHEN UPPER(TRIM(COALESCE("Pricing Model", ''))) NOT IN ('ABSORB', 'HYBRID', 'SUBMIT', 'NO PRICING') 
       AND TRIM(COALESCE("Pricing Model", '')) != '' THEN 1 ELSE 0 END AS pricing_model_other,
  CASE WHEN TRIM(COALESCE("Pricing Model", '')) = '' THEN 1 ELSE 0 END AS pricing_model_missing

FROM 
  CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"),

Select nm."PaymentIDBillerIDMerchantOrder",
  nm.original_name                        AS "Payment_CustomerName",
  nm.name_length,
  nm.name_too_short,
  nm.name_too_long,
  nm.name_word_count,
  nm.single_word_name,
  nm.is_all_caps,
  nm.is_all_lowercase,
  nm.is_business_entity,
  nm.is_test_name,
  nm.contains_test_keyword,
  nm.has_special_chars,
  nm.has_numbers,
  nm.first_name,
  nm.last_name,
  nm.name_frequency,
  nm.multiple_transactions_same_name,
  nm.has_numbers_not_business,
  nm.has_keyboard_pattern,
  nm.potential_shell,
  nm.first_last_name_same,
  nm.single_letter_first,
  nm.single_letter_last,
  nm.name_risk_score,
  CASE
    WHEN nm.name_risk_score <=  3 THEN 'Very Low'
    WHEN nm.name_risk_score <=  7 THEN 'Low'
    WHEN nm.name_risk_score <= 15 THEN 'Medium'
    ELSE                         'High'
  END                                   AS name_risk_category,

 em.original_email       AS "EmailAddress",
  em.norm_email           AS email_clean,
  em.missing_email,
  em.is_valid_format,
  em.username,
  em.domain,
  em.is_business_domain,
  em.is_gmail,
  em.is_yahoo,
  em.is_hotmail,
  em.is_outlook,
  em.is_free_email,
  em.is_disposable_email,
  em.username_length,
  em.missing_username,
  em.short_username,
  em.long_username,
  em.username_has_numbers,
  em.username_only_numbers,
  em.has_test_keyword,
  em.has_sequential_pattern,
  em.username_has_special_chars,
  em.is_role_account,
  em.has_numbers_not_business,
  em..email_frequency,
  em..reused_email,
  em..email_risk_score,
  CASE
    WHEN em.email_risk_score <=  3 THEN 'Very Low'
    WHEN em.email_risk_score <=  7 THEN 'Low'
    WHEN em.email_risk_score <= 15 THEN 'Medium'
    ELSE                            'High'
  END AS em..email_risk_category,

 ad.original_address                AS "Address1",
  ad.address_length,
  ad.missing_address,
  ad.address_too_short,
  ad.address_too_long,
  ad.address_word_count,
  ad.too_few_words,
  ad.is_po_box,
  ad.has_test_address_keyword,
  ad.is_generic_address,
  ad.has_invalid_chars,
  ad,all_caps,
  ad,has_high_risk_state,
  ad.potential_virtual_office,
  ad.numbers_only,
  ad.has_zip_format,
  ad.has_keyboard_pattern,
  ad.has_numbers_not_business,
  ad.address_frequency,
  ad.high_frequency_address,
  ad.address_risk_score,
  CASE
    WHEN ad.address_risk_score <=  3 THEN 'Very Low'
    WHEN ad.address_risk_score <=  7 THEN 'Low'
    WHEN ad.address_risk_score <= 15 THEN 'Medium'
    ELSE                             'High'
  END                                AS address_risk_category ,

ip.*,
cc.*,
ps.*,
mc.*,
pf.*,
geo.*,
ebpp.*
price.*



FROM Name_Scored nm left join 
Email_Scored em 
on nm."PaymentIDBillerIDMerchantOrder"=em."PaymentIDBillerIDMerchantOrder"
left join 
Address_Scored ad 
on nm."PaymentIDBillerIDMerchantOrder"=ad."PaymentIDBillerIDMerchantOrder"
left join IP_score ip
on nm."PaymentIDBillerIDMerchantOrder"=ip."PaymentIDBillerIDMerchantOrder"
left join card_score cc
on nm."PaymentIDBillerIDMerchantOrder"=cc."PaymentIDBillerIDMerchantOrder"
left join Payment_score ps
nm."PaymentIDBillerIDMerchantOrder"=ps."PaymentIDBillerIDMerchantOrder"
left join MCCRiskFeatures mc
nm."PaymentIDBillerIDMerchantOrder"=mc."PaymentIDBillerIDMerchantOrder"
left join Partner_Features pf
nm."PaymentIDBillerIDMerchantOrder"=pf."PaymentIDBillerIDMerchantOrder"
left join Geo_Features geo 
nm."PaymentIDBillerIDMerchantOrder"=geo."PaymentIDBillerIDMerchantOrder"
left join EBPPFeatures ebpp
nm."PaymentIDBillerIDMerchantOrder"=ebpp."PaymentIDBillerIDMerchantOrder"
left join Pricing_Features price
nm."PaymentIDBillerIDMerchantOrder"=price."PaymentIDBillerIDMerchantOrder"

LIMIT 100
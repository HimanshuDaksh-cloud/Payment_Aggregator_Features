WITH CardFeatures AS (
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

SELECT 
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
ORDER BY 
  card_risk_score DESC;
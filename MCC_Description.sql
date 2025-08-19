WITH MCCFeatures AS (
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

SELECT 
  mrf."PaymentIDBillerIDMerchantOrder",
  mrf.raw_mcc,
  mrf.mcc_clean,
  mrf.missing_mcc,
  mrf.is_high_risk_mcc,
  mrf.is_business_related_mcc,
  mrf.is_government_related,
  mrf.is_nec_category,
  mrf.is_unusual_mcc,
  mrf.has_numbers_not_business,  -- Direct equivalent to your has_numbers_not_business function
  mrf.has_special_chars,
  mrf.mcc_too_short,
  mrf.is_test_mcc,
  
  -- MCC frequency information
  COALESCE(mc.mcc_frequency, 0) AS mcc_frequency,
  COALESCE(mc.distinct_customers, 0) AS distinct_customers,
  COALESCE(mc.total_amount, 0) AS total_amount,
  CASE WHEN COALESCE(mc.mcc_frequency, 0) < 5 THEN 1 ELSE 0 END AS is_rare_mcc,
  
  -- MCC risk score calculation - follows your name_risk_score approach
  (
    -- Reduce risk scores for business-related MCCs (same approach as your code)
    (mrf.has_numbers_not_business * CASE WHEN mrf.is_business_related_mcc = 1 THEN 1 ELSE 3 END) +
    (mrf.has_special_chars * CASE WHEN mrf.is_business_related_mcc = 1 THEN 1 ELSE 2 END) +
    
    -- Higher risk category scores
    (mrf.is_high_risk_mcc * 4) +
    (mrf.is_government_related * 3) +
    
    -- Regular scores (using same weights as your code where applicable)
    (mrf.missing_mcc * 10) +
    (mrf.mcc_too_short * 3) +
    (mrf.mcc_too_long * 1) +
    (mrf.is_all_caps * 2) +
    (mrf.is_test_mcc * 10) +
    (mrf.is_unusual_mcc * 3) +
    (mrf.is_nec_category * 2) +
    (CASE WHEN COALESCE(mc.mcc_frequency, 0) < 5 THEN 6 ELSE 0 END) +
    (CASE WHEN COALESCE(mc.distinct_customers, 0) < 3 AND COALESCE(mc.mcc_frequency, 0) > 0 THEN 4 ELSE 0 END)
  ) AS mcc_risk_score,
  
  -- MCC risk category
  CASE 
    WHEN (
      (mrf.has_numbers_not_business * CASE WHEN mrf.is_business_related_mcc = 1 THEN 1 ELSE 3 END) +
      (mrf.has_special_chars * CASE WHEN mrf.is_business_related_mcc = 1 THEN 1 ELSE 2 END) +
      (mrf.is_high_risk_mcc * 4) +
      (mrf.is_government_related * 3) +
      (mrf.missing_mcc * 10) +
      (mrf.mcc_too_short * 3) +
      (mrf.mcc_too_long * 1) +
      (mrf.is_all_caps * 2) +
      (mrf.is_test_mcc * 10) +
      (mrf.is_unusual_mcc * 3) +
      (mrf.is_nec_category * 2) +
      (CASE WHEN COALESCE(mc.mcc_frequency, 0) < 5 THEN 6 ELSE 0 END) +
      (CASE WHEN COALESCE(mc.distinct_customers, 0) < 3 AND COALESCE(mc.mcc_frequency, 0) > 0 THEN 4 ELSE 0 END)
    ) <= 3 THEN 'Very Low'
    WHEN (
      (mrf.has_numbers_not_business * CASE WHEN mrf.is_business_related_mcc = 1 THEN 1 ELSE 3 END) +
      (mrf.has_special_chars * CASE WHEN mrf.is_business_related_mcc = 1 THEN 1 ELSE 2 END) +
      (mrf.is_high_risk_mcc * 4) +
      (mrf.is_government_related * 3) +
      (mrf.missing_mcc * 10) +
      (mrf.mcc_too_short * 3) +
      (mrf.mcc_too_long * 1) +
      (mrf.is_all_caps * 2) +
      (mrf.is_test_mcc * 10) +
      (mrf.is_unusual_mcc * 3) +
      (mrf.is_nec_category * 2) +
      (CASE WHEN COALESCE(mc.mcc_frequency, 0) < 5 THEN 6 ELSE 0 END) +
      (CASE WHEN COALESCE(mc.distinct_customers, 0) < 3 AND COALESCE(mc.mcc_frequency, 0) > 0 THEN 4 ELSE 0 END)
    ) <= 7 THEN 'Low'
    WHEN (
      (mrf.has_numbers_not_business * CASE WHEN mrf.is_business_related_mcc = 1 THEN 1 ELSE 3 END) +
      (mrf.has_special_chars * CASE WHEN mrf.is_business_related_mcc = 1 THEN 1 ELSE 2 END) +
      (mrf.is_high_risk_mcc * 4) +
      (mrf.is_government_related * 3) +
      (mrf.missing_mcc * 10) +
      (mrf.mcc_too_short * 3) +
      (mrf.mcc_too_long * 1) +
      (mrf.is_all_caps * 2) +
      (mrf.is_test_mcc * 10) +
      (mrf.is_unusual_mcc * 3) +
      (mrf.is_nec_category * 2) +
      (CASE WHEN COALESCE(mc.mcc_frequency, 0) < 5 THEN 6 ELSE 0 END) +
      (CASE WHEN COALESCE(mc.distinct_customers, 0) < 3 AND COALESCE(mc.mcc_frequency, 0) > 0 THEN 4 ELSE 0 END)
    ) <= 15 THEN 'Medium'
    ELSE 'High'
  END AS mcc_risk_category,
  
  -- MCC Type Classification (similar to your name type bifurcation)
  CASE
    -- Type 1: Missing, Test or Invalid MCCs (highest risk)
    WHEN mrf.missing_mcc = 1 OR 
         mrf.is_test_mcc = 1 OR
         mrf.mcc_too_short = 1
      THEN 'Type 1'
    
    -- Type 2: High Risk MCCs with unusual patterns (high risk)
    WHEN (mrf.is_high_risk_mcc = 1 OR mrf.is_government_related = 1) AND
         (mrf.has_numbers_not_business = 1 OR mrf.has_special_chars = 1 OR mrf.is_unusual_mcc = 1)
      THEN 'Type 2'
    
    -- Type 3: High Risk MCCs but with standard patterns (medium risk)
    WHEN mrf.is_high_risk_mcc = 1 OR mrf.is_government_related = 1
      THEN 'Type 3'
    
    -- Type 4: Standard MCCs (low risk)
    ELSE 'Type 4'
  END AS mcc_type

FROM 
  MCCRiskFeatures mrf
LEFT JOIN 
  MCCCounts mc ON mrf.mcc_normalized = mc.mcc
  -- Where "RAW_MCC" is not null
ORDER BY 
  mcc_risk_score DESC;
  
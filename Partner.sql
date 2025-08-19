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

FROM CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3" t1
ORDER BY "partner_risk_score" DESC;
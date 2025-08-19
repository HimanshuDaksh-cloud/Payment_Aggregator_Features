-- Query to extract risk features using RUCA codes with first 5 digits of ZIP code
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
WHERE t1."Zip" IS NOT NULL  -- Exclude records without ZIP codes
ORDER BY "geo_risk_score" DESC;
SELECT
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
  CONSOLIDATED_METRICS.SCRATCH."GTN_PaymentGrossToNetOverTime_V3"
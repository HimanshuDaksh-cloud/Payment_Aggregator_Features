WITH IPFeatures AS (
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
ORDER BY
ip_risk_score DESC;
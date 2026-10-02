-- =========================================================================
-- Bridge / Junction Tables DDL (linkedindimensionsfacts database)
-- =========================================================================

-- 1. Job-Skills Bridge Table
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.job_skills_dim (
    job_key BIGINT,
    skill_key BIGINT
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/job_skills_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 2. Job-Benefits Bridge Table
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.job_benefits_dim (
    job_key BIGINT,
    benefits_key BIGINT
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/job_benefits_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 3. Companies-Industries Bridge Table
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.companies_industries_dim (
    company_key BIGINT,
    industry_key BIGINT
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/companies_industries_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
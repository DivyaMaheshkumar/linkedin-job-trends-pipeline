-- =========================================================================
-- Fact Tables DDL (linkedindimensionsfacts database)
-- =========================================================================

-- 1. Job Postings Fact Table
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.job_postings_fact (
    postings_key BIGINT,
    job_key BIGINT,
    zip_key BIGINT,
    listed_date_key BIGINT,
    expiry_date_key BIGINT,
    max_salary DOUBLE,
    min_salary DOUBLE,
    med_salary DOUBLE,
    views INT,
    applies INT,
    active_post_days DOUBLE,
    skills_count INT,
    engagement_score DOUBLE
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/job_postings_fact_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
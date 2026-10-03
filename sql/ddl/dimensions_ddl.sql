-- =========================================================================
-- Dimension Tables DDL (linkedindimensionsfacts database)
-- =========================================================================

-- 1. Country Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.country_dim (
    country_key BIGINT,
    country_name STRING
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/country_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 2. State Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.state_dim (
    state_key BIGINT,
    state_name STRING,
    country_key BIGINT
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/state_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 3. City Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.city_dim (
    city_key BIGINT,
    city_name STRING,
    state_key BIGINT
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/city_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 4. Zip Dimension (Geographical Hierarchy)
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.zip_dim (
    zip_key BIGINT,
    zip_code STRING,
    city_key BIGINT
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/zip_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 5. Calendar Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.calendar (
    date_key INT,
    date DATE,
    month_id INT,
    month_name STRING,
    quarter INT,
    year INT
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/calendar/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 6. Companies Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.companies_dim (
    company_key BIGINT,
    company_name STRING,
    employee_count BIGINT,
    follower_count BIGINT
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/companies_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 7. Jobs Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.jobs_dim (
    job_key BIGINT,
    title STRING,
    work_type STRING,
    pay_period STRING,
    remote_allowed INT,
    experience_level STRING,
    company_key BIGINT
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/jobs_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 8. Skills Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.skills_dim (
    skill_key BIGINT,
    skill_abr STRING,
    skill_name STRING
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/skills_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 9. Benefits Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.benefits_dim (
    benefits_key BIGINT,
    type STRING
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/benefits_transformed/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- 10. Industries Dimension
CREATE EXTERNAL TABLE IF NOT EXISTS linkedindimensionsfacts.industries_dim (
    industry_key BIGINT,
    industry_name STRING
)
STORED AS PARQUET
LOCATION 's3://linkedinjobs24/integration_layer/industries/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
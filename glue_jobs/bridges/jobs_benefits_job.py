from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)

# --- Load Data ---
# Load job_dim table
job_dim_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="jobs_dim"
)
job_dim_df = job_dim_data.toDF()

# Load benefits_dim table
benefits_dim_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="benefits_dim"
)
benefits_dim_df = benefits_dim_data.toDF()

# Load benefits JSON data
benefits_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://linkedinjobs24/landing_zone/benefits/"]},
    format="json"
)
benefits_df = benefits_data.toDF()

# --- Handle Nested job_id Field ---
# Extract the correct field from job_id struct
benefits_df = benefits_df.withColumn(
    "job_id", benefits_df["job_id.long"].cast("bigint"))

# Join benefits JSON with jobs_dim to map job_id -> job_key
joined_benefits_job = benefits_df.join(
    job_dim_df,
    benefits_df["job_id"] == job_dim_df["job_key"],
    "inner"
)

# Join the result with benefits_dim to map type -> benefit_key
final_joined_df = joined_benefits_job.join(
    benefits_dim_df,
    joined_benefits_job["type"] == benefits_dim_df["type"],
    "inner"
)

# Select relevant columns for the Job-Benefits junction table
job_benefits_junction = final_joined_df.select(
    job_dim_df["job_key"],
    benefits_dim_df["benefits_key"]
).dropDuplicates(["job_key", "benefits_key"])

# --- Write Result to S3 ---
output_path = "s3://linkedinjobs24/integration_layer/job_benefits_transformed/"
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(job_benefits_junction,
                              glueContext, "job_benefits_junction"),
    connection_type="s3",
    connection_options={"path": output_path,
                        "partitionKeys": [], "mode": "overwrite"},
    format="parquet"
)

print("Job-Benefits junction table successfully written to:", output_path)

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)

# Initialize SparkSession
spark = SparkSession.builder.appName("JobsUpsert").getOrCreate()

# --- Load Job Postings Data from Glue Catalog ---
postings_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindw",
    table_name="postings"
)
postings_df = postings_data.toDF()

# --- Load Companies Dimension Data from Glue Catalog ---
companies_dim_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="companies_dim"
)
companies_dim_df = companies_dim_data.toDF()

# --- Load Jobs Dimension Data (Existing Jobs) ---
jobs_dim_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="jobs_dim"
)
jobs_dim_df = jobs_dim_data.toDF()

# --- Clean and Transform Postings Data ---
# Drop unnecessary columns and rename job columns as per the required structure
postings_df = postings_df.select(
    "job_id", "title", "formatted_work_type", "pay_period", "remote_allowed", "formatted_experience_level", "company_id"
)

# Rename columns to match the job table schema
postings_df = postings_df.withColumnRenamed("job_id", "job_key") \
                         .withColumnRenamed("formatted_work_type", "work_type") \
                         .withColumnRenamed("formatted_experience_level", "experience_level")

combined_df = postings_df.join(
    # Select only the company_key column from companies_dim_df
    companies_dim_df.select("company_key"),
    # Join on company_id from postings and company_key from companies_dim
    postings_df.company_id == companies_dim_df.company_key,
    "inner"  # Inner join to ensure we only keep rows with valid company_key
).drop("company_id")

# --- Identify Records to Update or Insert ---
# Join combined data with existing job data to find matching records (jobs already present)
updated_jobs_df = combined_df.join(
    jobs_dim_df,
    combined_df.job_key == jobs_dim_df.job_key,
    "inner"  # This is where the job_key matches
)

# Step 2: Replace the old columns in jobs_dim_df with the updated data
# Select the necessary columns from combined_df (these are the new values to update)
updated_jobs_df = updated_jobs_df.select(
    combined_df.job_key,
    combined_df.title,
    combined_df.work_type,
    combined_df.pay_period,
    combined_df.remote_allowed,
    combined_df.experience_level,
    jobs_dim_df.company_key  # Retain the company_key from jobs_dim_df
)

# Identify new jobs that do not exist in the jobs_dim table (using left anti join)
new_jobs_df = combined_df.join(
    jobs_dim_df.select("job_key"),
    combined_df.job_key == jobs_dim_df.job_key,
    "left_anti"
)

# --- Combine Updated and New Jobs Data ---
# Union the updated and new jobs data
final_df = updated_jobs_df.union(new_jobs_df)

final_df = final_df.coalesce(1)

# --- Convert the Final Combined DataFrame to DynamicFrame ---
combined_dynamic_frame = DynamicFrame.fromDF(
    final_df, glueContext, "combined_jobs")

# --- Write the Combined Data to S3 in Parquet Format ---
jobs_output_path = "s3://awsetlscripttest/jobs_transformed/"

glueContext.write_dynamic_frame.from_options(
    frame=combined_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": jobs_output_path,
        "partitionKeys": [],  # Specify partitioning columns here if needed
        "mode": "overwrite"  # Use overwrite for simplicity in this example
    },
    format="parquet"
)

print("Jobs successfully updated and inserted to S3!")

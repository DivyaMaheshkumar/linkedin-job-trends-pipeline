from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)

# Initialize SparkSession
spark = SparkSession.builder.appName("CompaniesUpsert").getOrCreate()

# --- Load Raw Companies Data from Glue Catalog ---
companies_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindw",
    table_name="companies"
)
companies_df = companies_data.toDF()

# --- Load Employee Counts Data from Glue Catalog ---
employee_counts_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindw",
    table_name="employee_counts"
)
employee_counts_df = employee_counts_data.toDF()

# Drop unnecessary columns and rename company_id to company_key
companies_df = companies_df.drop(
    "company_size", "state", "country", "city", "zipcode", "url")
companies_df = companies_df.withColumnRenamed("company_id", "company_key")

# --- Load Existing Data from S3 (Previous Load) ---
existing_data_path = "s3://linkedinjobs24/integration_layer/companies_transformed/"
existing_companies_df = spark.read.parquet(existing_data_path)

# --- Join Companies Data with Employee Counts ---
# Perform a left join on 'company_key' to get employee_count and follower_count from employee_counts_df
combined_df = companies_df.join(
    employee_counts_df.select(
        "company_id", "employee_count", "follower_count"),
    companies_df.company_key == employee_counts_df.company_id,
    "left_outer"  # Use 'left_outer' join to keep all companies, even if no match in employee_counts_df
)

# Drop the 'company_id' from employee_counts_df to avoid column duplication
combined_df = combined_df.drop("company_id")

# Update Records
# Find companies that are already in the existing data
updated_companies_df = existing_companies_df.join(
    combined_df.select("company_key"),
    "company_key",
    "inner"  # These are the companies that already exist in the existing data
)
# Insert Records
# These are the companies that are new and need to be inserted
new_companies_df = combined_df.join(
    existing_companies_df.select("company_key"),
    "company_key",
    "left_anti"  # These companies don't exist in the existing data
)

# --- Combine Updated and New Data ---
# We assume you want to update the existing records by replacing them with the new data
# So, we remove the existing companies and add the new and updated records
final_df = updated_companies_df.union(new_companies_df)

final_df = final_df.coalesce(1)

# --- Convert the Combined DataFrame to DynamicFrame ---
combined_dynamic_frame = DynamicFrame.fromDF(
    final_df, glueContext, "combined_companies")

# --- Write the Combined Data to S3 in Parquet Format ---
companies_output_path = "s3://linkedinjobs24/integration_layer/companies_transformed/"

glueContext.write_dynamic_frame.from_options(
    frame=combined_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": companies_output_path,
        "partitionKeys": [],  # Specify partitioning columns here if needed
        "mode": "overwrite"  # Use overwrite for simplicity in this example
    },
    format="parquet"
)

print("Companies successfully updated and inserted to S3!")

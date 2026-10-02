from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --- Load tables from Glue Catalog ---
# Load company_industries data
company_industries_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindw",
    table_name="company_industries"
)

company_industries_df = company_industries_data.toDF()

# Load companies data
companies_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindw",
    table_name="companies"
)

companies_df = companies_data.toDF()

# Load industries data
industries_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindw",
    table_name="industries"
)

industries_df = industries_data.toDF()

# --- Process Company Inductions to Bridge Table ---
# Rename company_id to company_key in companies_df
companies_df = companies_df.withColumnRenamed("company_id", "company_key")

# Rename industry_id to industry_key in industries_df
industries_df = industries_df.withColumnRenamed("industry_id", "industry_key")

# Join company_industries_df with companies_df on company_id to get company_key
company_industries_with_key = company_industries_df.join(
    companies_df,
    company_industries_df["company_id"] == companies_df["company_key"],
    "inner"
).drop("company_id")  # Drop the old company_id

# Join the result with industries_df to get industry_key
company_industries_with_keys = company_industries_with_key.join(
    industries_df,
    company_industries_with_key["industry"] == industries_df["industry_name"],
    "inner"
).drop("industry_name")  # Drop the industry_name column

# Select the desired columns: company_key and industry_key
bridge_table_df = company_industries_with_keys.select(
    "company_key", "industry_key")

# Consolidate the result into one file
bridge_table_df = bridge_table_df.coalesce(1)

# Print the schema and preview the data (optional)
bridge_table_df.printSchema()
bridge_table_df.show(5)

# Convert the resulting DataFrame to DynamicFrame for Glue
bridge_table_dynamic_frame = DynamicFrame.fromDF(
    bridge_table_df, glueContext, "company_industries_bridge")

# Specify output path for the bridge table
output_path = "s3://linkedinjobs24/integration_layer/company_industries/"

# Write the bridge table to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=bridge_table_dynamic_frame,
    connection_type="s3",
    connection_options={"path": output_path,
                        "partitionKeys": [], "mode": "overwrite"},
    format="parquet"
)

print("Company-Industries Bridge Table successfully written to:", output_path)

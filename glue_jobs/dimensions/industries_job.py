from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --- Process 'industries' table ---
# Load raw industries data from Glue Catalog
industries_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindw",
    table_name="industries"
)

industries_df = industries_data.toDF()

# Rename the industry_id column to industry_key
industries_df = industries_df.withColumnRenamed("industry_id", "industry_key")

# Consolidate data into one file
industries_df = industries_df.coalesce(1)  # Ensures only 1 output file

# Print schema and preview data (optional)
industries_df.printSchema()
industries_df.show(5)

# Convert DataFrame to DynamicFrame
industries_transformed = DynamicFrame.fromDF(
    industries_df, glueContext, "industries_transformed")

# Specify output path for transformed industries data
industries_output_path = "s3://linkedinjobs24/integration_layer/industries_transformed/"

# Write the transformed industries data to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=industries_transformed,
    connection_type="s3",
    connection_options={"path": industries_output_path,
                        "partitionKeys": [], "mode": "overwrite"},
    format="parquet"
)

print("Industries Dimension successfully written to:", industries_output_path)

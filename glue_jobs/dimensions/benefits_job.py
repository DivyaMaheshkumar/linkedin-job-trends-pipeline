from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, trim, row_number
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Load data from Glue Catalog
benefits_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindw",
    table_name="benefits"
)
benefits_df = benefits_data.toDF()

# --- Clean & Deduplicate to Build a Proper Dimension Table ---
# 1. Trim whitespace, filter out nulls/empties, and get distinct types
cleaned_benefits = benefits_df \
    .withColumn("cleaned_type", trim(col("type"))) \
    .filter("cleaned_type IS NOT NULL AND cleaned_type != ''") \
    .select("cleaned_type") \
    .distinct()

# 2. Assign clean, sequential surrogate keys (benefits_key)
windowSpec = Window.orderBy("cleaned_type")
benefits_dim_df = cleaned_benefits \
    .withColumn("benefits_key", row_number().over(windowSpec)) \
    .withColumnRenamed("cleaned_type", "type") \
    .select("benefits_key", "type")

# 3. Coalesce to a single partition for optimal Athena query performance
benefits_dim_single = benefits_dim_df.coalesce(1)

# Convert back to DynamicFrame for Glue compatibility
benefits_transformed = DynamicFrame.fromDF(
    benefits_dim_single, glueContext, "benefits_transformed")

# Overwrite the existing data in the integration layer
output_path = "s3://linkedinjobs24/integration_layer/benefits_transformed/"
glueContext.write_dynamic_frame.from_options(
    frame=benefits_transformed,
    connection_type="s3",
    connection_options={"path": output_path,
                        "partitionKeys": [], "mode": "overwrite"},
    format="parquet"
)

print("Benefits dimension table cleaned, deduplicated, and written to:", output_path)

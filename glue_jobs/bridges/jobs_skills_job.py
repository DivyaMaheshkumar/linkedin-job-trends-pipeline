
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job_skills_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindw",
    table_name="job_skills"
)
job_skills_df = job_skills_data.toDF()
skills_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindw",
    table_name="skills"
)
skills_df = skills_data.toDF()
jobs_dim_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="jobs_dim"
)
jobs_dim_df = jobs_dim_data.toDF()

skills_dim_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="skills_dim"
)
skills_dim_df = skills_dim_data.toDF()
# --- Transformations and Joins ---
# 1. Join job_skills with jobs_dim using job_id == job_key to get the correct job_key
joined_jobs = job_skills_df.join(
    jobs_dim_df,
    job_skills_df["job_id"] == jobs_dim_df["job_key"],
    "inner"
)

# 2. Join with skills_dim (or skills raw) using skill_abr to bring in the skill_key
joined_skills = joined_jobs.join(
    skills_dim_df,
    joined_jobs["skill_abr"] == skills_dim_df["skill_abr"],
    "inner"
)

# 3. Select only the final surrogate keys required by job_skills_dim and deduplicate
job_skills_junction = joined_skills.select(
    "job_key",
    "skill_key"
).dropDuplicates(["job_key", "skill_key"])
# Consolidate into a single partition file
job_skills_junction_single = job_skills_junction.coalesce(1)

# --- Write Result to S3 in Parquet Format ---
output_path = "s3://linkedinjobs24/integration_layer/job_skills_transformed/"

job_skills_dynamic_frame = DynamicFrame.fromDF(
    job_skills_junction_single,
    glueContext,
    "job_skills_junction"
)

glueContext.write_dynamic_frame.from_options(
    frame=job_skills_dynamic_frame,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": [],
        "mode": "overwrite"
    },
    format="parquet"
)

print("Job-Skills dimension table successfully written to:", output_path)
s3output = glueContext.getSink(
    path="s3://bucket_name/folder_name",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
)
s3output.setCatalogInfo(
    catalogDatabase="demo", catalogTableName="populations"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
job.commit()

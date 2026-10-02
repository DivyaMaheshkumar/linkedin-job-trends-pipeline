from pyspark.sql import functions as F
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.window import Window

# Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)

postings_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://linkedinjobs24/landing_zone/postings/"]},
    format="parquet"
)
postings_df = postings_data.toDF()

# Load jobs_dim table (job_key)
jobs_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="jobs_dim"
)
jobs_df = jobs_data.toDF()

# Load zip_dim table (zip_key)
zip_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="zip_dim"
)
zip_df = zip_data.toDF()

# Load calendar_dim table (date_key)
calendar_data_listed_time = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="calendar"
)
calendar_listed_time_df = calendar_data_listed_time.toDF()

calendar_data_expiry = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="calendar"
)
calendar_expiry_df = calendar_data_expiry.toDF()

# Load job_skills_dim table (skills associated with job_key)
job_skills_data = glueContext.create_dynamic_frame.from_catalog(
    database="linkedindimensionsfacts",
    table_name="job_skills_dim"
)
job_skills_df = job_skills_data.toDF()

# --- Cast zip_code to long (before join) ---
postings_df_casted = postings_df.withColumn(
    "zip_code",
    postings_df["zip_code"].cast("long")
).withColumnRenamed("pay_period", "pay_period_from_postings")

# --- Join Postings with Jobs Dimension (on job_id and job_key) ---
joined_postings_jobs = postings_df_casted.join(
    jobs_df,
    postings_df_casted["job_id"] == jobs_df["job_key"],
    "inner"
)

# --- Join Postings with Zip Dimension (on zip_code and zip_key) ---
joined_postings_zip = joined_postings_jobs.join(
    zip_df,
    joined_postings_jobs["zip_code"] == zip_df["zip_key"],
    "left"
)

joined_postings_calendar_listed = joined_postings_zip.join(
    # Rename date_key in calendar_listed_df
    calendar_listed_time_df.withColumnRenamed("date_key", "listed_date_key"),
    F.to_date(F.from_unixtime(
        joined_postings_zip["listed_time"] / 1000)) == calendar_listed_time_df["date"],
    "left"
)

# Join Postings with Calendar Dimension (on expiry and date)
joined_postings_calendar_expiry = joined_postings_calendar_listed.join(
    # Rename date_key in calendar_expiry_df
    calendar_expiry_df.withColumnRenamed("date_key", "expiry_date_key"),
    F.to_date(F.from_unixtime(
        joined_postings_calendar_listed["expiry"] / 1000)) == calendar_expiry_df["date"],
    "left"
)

# Count Records per Job Key from Job Skills Dimension
job_skills_count_df = job_skills_df.groupBy("job_key").agg(
    F.count("job_key").alias("skills_count")
).withColumnRenamed("job_key", "skills_job_key")

# Join the job_skills_count back to the final_df
final_df_with_skills_count = joined_postings_calendar_expiry.join(
    job_skills_count_df,
    joined_postings_calendar_expiry["job_key"] == job_skills_count_df["skills_job_key"],
    "left"
)

# First, calculate the date values and store them in variables
listed_date = F.from_unixtime(F.col("listed_time") / 1000).cast("date")
expiry_date = F.from_unixtime(F.col("expiry") / 1000).cast("date")

# Then, use these variables to calculate the difference in days
final_df = final_df_with_skills_count.withColumn(
    "listed_date", listed_date
).withColumn(
    "expiry_date", expiry_date
).withColumn(
    "active_post_days", F.datediff(expiry_date, listed_date).cast(
        "double")  # Subtract dates to get difference in days
)

# Calculate active_post_days as the difference between expiry and listed date
final_df = final_df.withColumn(
    "active_post_days",
    F.when(
        (F.col("expiry_date").isNotNull()) & (
            F.col("listed_date").isNotNull()),
        (F.datediff(F.col("expiry_date"), F.col("listed_date")).cast("double"))
    ).otherwise(None)
)


def convert_salary_based_on_frequency(salary, frequency):
    return F.when(
        # Assuming 40 hours per week, 52 weeks a year
        frequency == 'HOURLY', salary * 2080
    ).when(
        frequency == 'MONTHLY', salary * 12
    ).when(
        frequency == 'BIWEEKLY', salary * 26  # Assuming 26 bi-weekly periods
    ).when(
        frequency == 'WEEKLY', salary * 52
    ).otherwise(salary)  # If 'YEARLY' or None, just use the salary as is.


final_df = final_df.withColumn(
    "max_salary",
    convert_salary_based_on_frequency(
        F.col("max_salary"), F.col("pay_period_from_postings"))
).withColumn(
    "min_salary",
    convert_salary_based_on_frequency(
        F.col("min_salary"), F.col("pay_period_from_postings"))
)

# --- Calculate med_salary (if null, calculate average) ---
final_df = final_df.withColumn(
    "med_salary",
    F.when(
        final_df["med_salary"].isNull(),
        # Case 1: Both max_salary and min_salary are non-null -> calculate average
        F.when(
            (final_df["max_salary"].isNotNull()) & (
                final_df["min_salary"].isNotNull()),
            (final_df["max_salary"] + final_df["min_salary"]) / 2
        ).otherwise(
            # Case 2: If only max_salary is non-null, use max_salary as med_salary
            F.when(final_df["max_salary"].isNotNull(), final_df["max_salary"])
            .otherwise(
                # Case 3: If only min_salary is non-null, use min_salary as med_salary
                F.when(final_df["min_salary"].isNotNull(),
                       final_df["min_salary"])
                # Case 4: If both are null, set med_salary to null
                .otherwise(None)
            )
        )
    ).otherwise(final_df["med_salary"])
)

# --- Calculate engagement_score (if applies and views are not null) ---
final_df = final_df.withColumn(
    "engagement_score",
    F.when(
        (final_df["applies"].isNotNull()) & (final_df["views"].isNotNull()),
        F.round(final_df["applies"] / final_df["views"], 2)
    ).otherwise(None)
)

window_spec = Window.orderBy(F.lit(1))
final_df = final_df.withColumn(
    "postings_key",
    F.row_number().over(Window.orderBy(F.lit(1))).cast("long")
)

# --- Select Relevant Columns ---
final_df = final_df.select(
    "postings_key",
    "job_key",
    "zip_key",
    "listed_date_key",
    "expiry_date_key",
    "max_salary",
    "min_salary",
    "med_salary",
    "views",
    "applies",
    "active_post_days",
    "skills_count",
    "engagement_score"
).withColumn(
    "views", F.col("views").cast("int")
).withColumn(
    "applies", F.col("applies").cast("int")
).withColumn(
    "skills_count", F.col("skills_count").cast("int")
)

# --- Coalesce the DataFrame to a Single Partition (Optional) ---
final_df_single_partition = final_df.coalesce(1)

# --- Convert DataFrame to DynamicFrame ---
final_dynamic_frame = DynamicFrame.fromDF(
    final_df_single_partition, glueContext, "jobs_transformed")

# --- Write the Result to S3 in Parquet Format ---
jobs_transformed_output_path = "s3://linkedinjobs24/integration_layer/job_postings_fact_transformed/"

glueContext.write_dynamic_frame.from_options(
    frame=final_dynamic_frame,
    connection_type="s3",
    connection_options={"path": jobs_transformed_output_path,
                        "partitionKeys": [], "mode": "overwrite"},
    format="parquet"
)

print("Job postings successfully written to:", jobs_transformed_output_path)

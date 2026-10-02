from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName('GenerateDateDimension').getOrCreate()

# Generate a range of dates from '2024-03-24' to '2024-11-17'
start_date = datetime(2024, 3, 24)
end_date = datetime(2024, 11, 17)

# Function to generate a list of dates


def generate_date_range(start_date, end_date):
    delta = timedelta(days=1)
    current_date = start_date
    date_range = []
    while current_date <= end_date:
        date_range.append(current_date.strftime('%Y-%m-%d'))
        current_date += delta
    return date_range


# Create a list of dates between the given range
date_list = generate_date_range(start_date, end_date)

# Convert the list to a DataFrame
date_df = spark.createDataFrame([(date,) for date in date_list], ['date'])

window_spec = Window.orderBy('date')

# Add additional columns: date_key, month_id, month_name, quarter, year
date_df = date_df.withColumn('date_key', F.row_number().over(window_spec).cast("int")) \
                 .withColumn('date', F.to_date('date', 'yyyy-MM-dd')) \
                 .withColumn('month_id', F.month('date')) \
                 .withColumn('month_name', F.date_format('date', 'MMMM')) \
                 .withColumn('quarter', F.when(F.month('date').between(1, 3), 1)
                             .when(F.month('date').between(4, 6), 2)
                             .when(F.month('date').between(7, 9), 3)
                             .otherwise(4)) \
                 .withColumn('year', F.year('date'))

# Reorder the columns to ensure date_key is the first column
date_df = date_df.select(
    'date_key',  # Place date_key first
    'date',
    'month_id',
    'month_name',
    'quarter',
    'year'
)

# Coalesce the DataFrame to a single partition (important for small outputs)
date_df_single_partition = date_df.coalesce(1)

# Show the DataFrame (for verification)
date_df_single_partition.show(truncate=False)

# Optional: Write the result to S3 as Parquet (or you can choose other formats like CSV)
output_path = 's3://linkedinjobs24/integration_layer/calendar/'
date_df_single_partition.write.mode('overwrite').parquet(output_path)

print(f"Date dimension table successfully written to: {output_path}")

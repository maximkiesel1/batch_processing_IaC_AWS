from pyspark.sql import SparkSession
from pyspark.sql.functions import col, posexplode, expr,when

# Start PySpark session
spark = SparkSession.builder \
    .appName("CSV Transformation") \
    .master("local[*]") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Read the data
df = spark.read.csv(
    '/Users/maximkiesel/batch_processing_IaC_AWS/data/20230703_measurement_data.csv',
    header=True,
    inferSchema=True
)

# sorting the data by 'start_time' ascending
sorted_df = df.orderBy(col("start_time").asc())

# Calculate the time delta per sample
sorted_df = sorted_df.withColumn(
    "timedelta_per_sample",
    when(col("samples").isNotNull() & (col("samples") != 0),
         (col("end_time") - col("start_time")) / col("samples")).otherwise(expr("INTERVAL 0 SECONDS"))
)

# Generate an array of integers from 0 to the maximum number of samples minus 1
df_expanded = sorted_df.withColumn(
    "idx_array",
    expr("sequence(0, samples - 1)")
)

# Calculate the expanded start_timestamp and end_timestamp values based on the idx_array column
df_expanded = df_expanded.withColumn(
    "expanded_rows",
    expr(
        """
        transform(idx_array, idx -> struct(
            start_time + idx * timedelta_per_sample AS start_time,
            start_time + (idx + 1) * timedelta_per_sample AS end_time
        ))
        """
    )
)

# Drop the idx_array column
df_expanded = df_expanded.drop("idx_array")


# Explode the 'expanded_rows' array column into multiple rows
df_explode = df_expanded.select("*", posexplode("expanded_rows").alias("index", "exploded_timestamps"))

# Extract start_timestamp and end_timestamp from the exploded 'timestamps' struct column
df_extracted = df_explode.withColumn(
    "start_time",
    col("exploded_timestamps").getField("start_time")
).withColumn(
    "end_time",
    col("exploded_timestamps").getField("end_time")
)

# Drop unnecessary columns
columns_to_drop = [
    "expanded_rows",
    "index",
    "samples",
    "timedelta_per_sample",
    "exploded_timestamps"
]
df_extracted = df_extracted.drop(*columns_to_drop)

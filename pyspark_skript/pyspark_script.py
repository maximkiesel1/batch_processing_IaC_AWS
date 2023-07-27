import sys
import re
from datetime import datetime
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, posexplode, expr, when
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime


spark = SparkSession.builder \
    .appName("batch-processing") \
    .getOrCreate()

# Get the Glue job arguments
args = getResolvedOptions(sys.argv, ['S3_BUCKET_SOURCE', 'S3_BUCKET_TARGET'])


# Initialize the S3 client
s3_client = boto3.client('s3',
                         region_name="eu-north-1"
                         )

# List CSV files in the source bucket
source_bucket_path = f's3://{args["S3_BUCKET_SOURCE"]}'
paginator = s3_client.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=args["S3_BUCKET_SOURCE"], Prefix='')

df = spark.read.csv(f"s3://{args['S3_BUCKET_SOURCE']}/20230703_measurement_data.csv", header=True, inferSchema=True)

csv_files = []
for page in pages:
    for obj in page['Contents']:
        if obj['Key'].endswith('.csv'):
            csv_files.append(f's3://{args["S3_BUCKET_SOURCE"]}/{obj["Key"]}')

# Get the latest CSV file based on the date in the filename
latest_date = None
latest_csv_file = None

for csv_file in csv_files:
    date_match = re.search(r'(\d{8})', csv_file)
    if date_match:
        date_str = date_match.group(1)
        try:
           file_date = datetime.strptime(date_str, '%Y%m%d')
           if not latest_date or file_date > latest_date:
                latest_date = file_date
                latest_csv_file = csv_file
        except ValueError:
            print(f"Invalid date format in CSV file {csv_file}")

# Read the data from the latest CSV file
if latest_csv_file:
    df = spark.read.csv(
        latest_csv_file,
        header=True,
        inferSchema=True
    )
else:
    print("No valid CSV files found in the bucket")


# sorting the data by 'start_time' ascending
sorted_df = df.orderBy(col("start_time").asc())

# Calculate the time delta per sample
sorted_df = sorted_df.withColumn(
    "timedelta_per_sample",
    when(col("samples").isNotNull() & (col("samples") != 0),
         (unix_timestamp(col("end_time")) - unix_timestamp(col("start_time"))) / col("samples")).otherwise(0)
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
        f"""
        transform(idx_array, idx -> struct(
            from_unixtime(unix_timestamp(start_time) + idx * timedelta_per_sample) AS start_time,
            from_unixtime(unix_timestamp(start_time) + (idx + 1) * timedelta_per_sample) AS end_time
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

# write in s3 as parquet file
df_extracted.write.option("ignoreDataLocality", "true").parquet(
    f"s3://{args['S3_BUCKET_TARGET']}/output/",
    mode='append'
)

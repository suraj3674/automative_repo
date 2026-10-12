
import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

# 1. Initialize Glue job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 2. Define S3 paths
raw_path = "s3://automative-bucket1/API_VEHICLE_SALES/raw/"
silver_path = "s3://automative-bucket1/API_VEHICLE_SALES/silver/vehicles/"

# 3. Read CSV files using a bookmark-enabled DynamicFrame
raw_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [raw_path],
        "recurse": True
    },
    format="csv",
    format_options={
        "withHeader": True,
        "separator": ","
    },
    transformation_ctx="raw_vehicle_csv_source"
)

# Convert to Spark DataFrame for transformation
df = raw_dyf.toDF()

print("Records read in this run:", df.count())
df.show(10, truncate=False)

# 4. Add derived column
df_transformed = df.withColumn(
    "production_year",
    F.year(
        F.to_date(F.col("production_date"), "yyyy-MM-dd")
    )
)

# 5. Write output to SILVER as Parquet
if df_transformed.take(1):
    (
        df_transformed.write
        .mode("append")
        .format("parquet")
        .save(silver_path)
    )

print("Silver output path:", silver_path)

# 6. Commit bookmark state after successful processing
job.commit()

print("Glue job and bookmark commit completed successfully.")


import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

try:
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

    # 4. Convert to Spark DataFrame
    df = raw_dyf.toDF()

    # Normalize column names by removing whitespace and BOM
    for column_name in df.columns:
        cleaned_name = column_name.replace("\ufeff", "").strip()
        if cleaned_name != column_name:
            df = df.withColumnRenamed(column_name, cleaned_name)

    print("Columns found:", df.columns)
    df.printSchema()

    record_count = df.count()
    print("Records read in this run:", record_count)
    df.show(10, truncate=False)

    # 5. Validate required columns
    required_columns = [
        "vehicle_id",
        "model",
        "plant",
        "production_date",
        "quantity"
    ]

    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}. "
            f"Actual columns: {df.columns}. "
            "Check the CSV header and Glue CSV parsing options."
        )

    # 6. Transform production_date and derive production_year
    df_transformed = df.withColumn(
        "production_date",
        F.to_date(
            F.trim(F.col("production_date").cast("string")),
            "yyyy-MM-dd"
        )
    ).withColumn(
        "production_year",
        F.year(F.col("production_date"))
    )

    # 7. Check transformed data
    df_transformed.show(10, truncate=False)

    # 8. Write output to SILVER as Parquet
    if record_count > 0:
        (
            df_transformed.write
            .mode("append")
            .format("parquet")
            .save(silver_path)
        )
        print("Silver data written successfully.")
    else:
        print("No new records to write.")

    print("Silver output path:", silver_path)

    # 9. Commit bookmark state only after successful processing
    job.commit()
    print("Glue job and bookmark commit completed.")

except Exception as error:
    print("Glue job failed:", str(error))
    raise

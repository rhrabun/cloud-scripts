import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Spark session initialization for AWS Glue-compatible operations
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

logger = glueContext.get_logger()

# Make sure to set below Glue Job Parameters to enable Iceberg support
# KEY                   VALUE
# --datalake-formats    iceberg
# --conf                spark.sql.catalog.glue_catalog.warehouse=s3://BUCKET_NAME/   --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO


# Make sure to set Glue Job Parameters or pass them with "Run with parameters"
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "DATABASE_NAME",
        "TABLE_NAME",
        "SOURCE_DATA_PATH",
        "WAREHOUSE_PATH",
        "PARTITION_KEYS",
    ],
)
job.init(args["JOB_NAME"], args)

csv_path = args["SOURCE_DATA_PATH"]
warehouse_path = args["WAREHOUSE_PATH"]
database_name = args["DATABASE_NAME"]
table_name = args["TABLE_NAME"]
partition_keys = args['PARTITION_KEYS'] # Can be one key or list of keys

try:
    logger.info(f"Creating database: {database_name}")
    # Make sure to grant permissions for Glue IAM role in Lake Formation - Permissions - Data locations
    spark.sql(
        f"""
        CREATE DATABASE IF NOT EXISTS glue_catalog.{database_name}
        LOCATION '{warehouse_path}/{database_name}.db'
    """
    )
    logger.info("Finished creating database")

    logger.info(f'Reading data from file: "{csv_path}"')
    df = (
        spark.read.option("delimiter", ";")
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
    )
    logger.info(f"Got {df.count()} records")

    logger.info(f'Writing records to the "{database_name}.{table_name}" table')
    df.writeTo(f"glue_catalog.{database_name}.{table_name}").tableProperty(
        "format-version", "2"
    ).partitionedBy(partition_keys).using("iceberg").createOrReplace()

except Exception as e:
    logger.error("Error:")
    logger.error(e)

job.commit()

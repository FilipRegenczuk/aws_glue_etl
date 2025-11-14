import os
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import pyspark.sql.functions as F


S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_STAGING_DIR_PATH = os.getenv('S3_STAGING_DIR_PATH')

# set script params
RAW_DB_NAME = "raw_db"
STAGING_DB_NAME = "staging_db"
RAW_TABLE_PREFIX = "raw_"
TABLES = [
    "products",
    "sales"
]


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


for table in TABLES:
    dyf = glueContext.create_dynamic_frame.from_catalog(database=RAW_DB_NAME, table_name=f'{RAW_TABLE_PREFIX}{table}')
    df = dyf.toDF()

    cleaned_df = df
    # Clean all string columns
    string_columns = [field.name for field in df.schema.fields 
                    if str(field.dataType) == 'StringType()']

    for column in string_columns:
        # Remove tabs
        cleaned_df = cleaned_df.withColumn(
            column,
            F.regexp_replace(F.col(column), '\t', ' ')
        )
        
        # Trim whitespace
        cleaned_df = cleaned_df.withColumn(
            column,
            F.trim(F.col(column))
        )
        
        # Remove multiple spaces
        cleaned_df = cleaned_df.withColumn(
            column,
            F.regexp_replace(F.col(column), '  +', ' ')
        )
        
        # Replace empty strings with None
        cleaned_df = cleaned_df.withColumn(
            column,
            F.when((F.col(column) == '') | (F.col(column) == 'null'), None)
            .otherwise(F.col(column))
        )

    cleaned_dyf = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_products")

    s3output = glueContext.getSink(
    path=f"s3://{S3_BUCKET_NAME}/{S3_STAGING_DIR_PATH}/{table}",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="s3output",
    )
    s3output.setCatalogInfo(
    catalogDatabase=STAGING_DB_NAME, catalogTableName=table
    )
    s3output.setFormat("glueparquet")
    s3output.writeFrame(cleaned_dyf)
    

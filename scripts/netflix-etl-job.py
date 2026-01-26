import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from pyspark.sql.functions import split, col
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


raw_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
    database="netflix_db",
    table_name="netflix",
    transformation_ctx="raw_dynamic_frame"
)

df = raw_dynamic_frame.toDF()


df_clean = df.filter(col("line").contains(",")) \
    .withColumn("UserID", split(col("line"), ",")[0]) \
    .withColumn("Rating", split(col("line"), ",")[1].cast("int")) \
    .withColumn("Date", split(col("line"), ",")[2])


DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

EvaluateDataQuality().process_rows(
    frame=glueContext.create_dynamic_frame.from_dataframe(df_clean, "df_clean"),
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)


glueContext.write_dynamic_frame.from_options(
    frame=glueContext.create_dynamic_frame.from_dataframe(df_clean, "df_clean"),
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://netflix-analytics-williams/processed/netflix/", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node"
)

job.commit()

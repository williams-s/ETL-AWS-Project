from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import LongType, StringType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("netflix-transform-job", {})


rating_input_path = "s3://netflix-analytics-williams2/raw/ratings/*.txt"
movie_input_path  = "s3://netflix-analytics-williams2/raw/movies/*.csv"

rating_output_path = "s3://netflix-analytics-williams2/processed/ratings/"
movie_output_path  = "s3://netflix-analytics-williams2/processed/movies/"

print("Processing ratings files...")
rating_rdd = sc.textFile(rating_input_path)

def parse_rating_lines(lines):
    """Parse le format Netflix ratings"""
    current_movie_id = None
    result = []
    for line in lines:
        line = line.strip()
        if line.endswith(':'):
            current_movie_id = line[:-1]
        elif line:
            parts = line.split(',')
            if len(parts) == 3 and current_movie_id:
                customer_id, rating, date = parts
                result.append(Row(
                    movie_id=current_movie_id,
                    customer_id=int(customer_id),
                    rating=int(rating),
                    date=date
                ))
    return result

rating_rows_rdd = rating_rdd.mapPartitions(parse_rating_lines)
rating_df = spark.createDataFrame(rating_rows_rdd)


rating_df = rating_df.select(
    col('movie_id').cast(LongType()),
    col('customer_id').cast(LongType()),
    col('rating').cast(LongType()),
    to_date(col('date'), 'yyyy-MM-dd').alias('rating_date')
)


print("\nProcessing movies files...")
movie_df = spark.read.csv(
    movie_input_path,
    inferSchema=True
)

movie_df = movie_df.select(
    col('_c0').alias('movie_id').cast(LongType()),
    col('_c1').alias('year').cast(LongType()),
    col('_c2').alias('title').cast(StringType()),
)

rating_df.write.mode('overwrite').parquet(rating_output_path)
movie_df.write.mode('overwrite').parquet(movie_output_path)

joined_df = rating_df.join(movie_df, on='movie_id', how='left')

joined_output_path = "s3://netflix-analytics-williams2/processed/joined/"
joined_df.write.mode('overwrite').parquet(joined_output_path)
job.commit()

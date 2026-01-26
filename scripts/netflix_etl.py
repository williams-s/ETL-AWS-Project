from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date 
from pyspark.sql.types import LongType 

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("netflix_transform_folder", {})

input_path = "s3://netflix-analytics-williams2/raw/netflix/*" 
output_path = "s3://netflix-analytics-williams2/processed/netflix/"  

rdd = sc.textFile(input_path)

def parse_lines(lines):
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
                    MovieID=current_movie_id,
                    CustomerID=int(customer_id),
                    Rating=int(rating),
                    Date=date
                ))
    return result

rows_rdd = rdd.mapPartitions(lambda partition: parse_lines(partition))
df = spark.createDataFrame(rows_rdd)

df = df.select(
    col('MovieID').cast(LongType()).alias('movie_id'),
    col('CustomerID').alias('customer_id'),
    col('Rating').alias('rating'),
    to_date(col('Date'), 'yyyy-MM-dd').alias('date')
)

df.write.mode('overwrite').parquet(output_path)

job.commit()

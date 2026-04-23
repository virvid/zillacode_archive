from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
	# Write code here
    spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()
    spark.sql("DROP TABLE IF EXISTS input_tempView")
    input_df.createTempView('input_tempView')

    query = """
    select 
        duration,
        genre,
        release_year,
        title,
        video_id,
        view_count
    from input_tempView
    where view_count > 1000000 and release_year >= 2019
    """
    
	return spark.sql(query)
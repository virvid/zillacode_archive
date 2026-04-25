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
	
	input_df.createOrReplaceTempView("input_tv")
	
	query = """
	select
	    concat('******', right(phone, 4)) as anon_phone,
		regexp_extract(email, '@(.*)', 1) as email_domain,
		user_id
	from input_tv
	order by user_id
	"""
	
	return spark.sql(query)
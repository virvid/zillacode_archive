from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(properties_df, landlords_df):
	# Write code here
    spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

    properties_df.createOrReplaceTempView('properties_tv')
    landlords_df.createOrReplaceTempView('landlords_tv')

    query = """
    with income as (
    select
        landlord_id,
        sum(rent) as total_rental_income
    from properties_tv
    group by landlord_id
    )
    
    select
        l.landlord_id,
        concat(l.first_name, ' ', l.last_name) as landlord_name,
        i.total_rental_income
    from
        landlords_tv l
    join
        income i
    on l.landlord_id = i.landlord_id
    """
    
	return spark.sql(query)
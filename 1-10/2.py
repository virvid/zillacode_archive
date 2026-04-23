from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(customers, orders, products):
	# Write code here
    spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

    customers.createOrReplaceTempView('customers_tv')
    orders.createOrReplaceTempView('orders_tv')
    products.createOrReplaceTempView('products_tv')

    query = """
    select
        p.category,
        concat(c.first_name, ' ', c.last_name) as customer_name,
        c.email,
        o.order_date,
        o.order_id,
        p.product_name
    from
        customers_tv c
    join
        orders_tv o on c.customer_id = o.customer_id
    join
        products_tv p on o.product_id = p.product_id
    """

    return spark.sql(query)
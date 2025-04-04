from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
from pyspark import SparkContext


sc = SparkContext()
sc.setLogLevel("WARN")

spark = SparkSession.builder \
.master('local[*]') \
.appName('PySparkShell') \
.getOrCreate()



input_df = spark.read \
.format('csv') \
.option('header','true') \
.option('inferSchema','true') \
.load('orders_sh.csv')

input_df.show(5)
order_df = input_df.groupby('order_status') \
.count()
order_df.show()




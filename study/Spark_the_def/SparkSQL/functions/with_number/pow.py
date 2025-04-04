from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, pow, col

spark = SparkSession.builder. \
    master("local[*]"). \
    appName("huy dep zai").getOrCreate()

df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Spark_the_def/Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv")

fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"),
2) + 5
df.select(expr("CustomerId"),
fabricatedQuantity.alias("realQuantity")).show(2)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, round, bround

spark = SparkSession.builder. \
    master("local[*]"). \
    appName("huy dep zai").getOrCreate()

df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Spark_the_def/Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv")

df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)

# round = lam tron len
# bround = lam tron xuong
# .cast(IntegerType()) = lam tron len

import random

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Dat dep zai") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

rdd = spark.sparkContext.parallelize(range(1,11)) \
    .map(lambda x: (x, random.randint(0, 99) * x))

print(rdd.collect())

df_no_name = spark.createDataFrame(rdd).show()
df = spark.createDataFrame(rdd, ["key", "value"]).show()

# Tạo RDD nhớ có schema => StructType(Array(StructField, StructField))



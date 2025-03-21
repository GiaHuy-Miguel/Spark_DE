from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder. \
    master("local[*]"). \
    appName("huy dep zai").getOrCreate()

df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Spark_the_def/Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv")

# df.printSchema()
# df.createOrReplaceTempView("hehe")

df.where(col("InvoiceNo") != 536365)\
.select("InvoiceNo", "Description")\
.show(5, False)

df.where("InvoiceNo = 536365") \
.show(5, False)

df.where("InvoiceNo <> 536365") \
.show(5, False)
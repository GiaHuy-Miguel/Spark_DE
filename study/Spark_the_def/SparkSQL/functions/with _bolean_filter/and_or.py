from pyspark.sql import SparkSession
from pyspark.sql.functions import col, instr

spark = SparkSession.builder. \
    master("local[*]"). \
    appName("huy dep zai").getOrCreate()

df = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Spark_the_def/Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv")

# and = .where 2 times
# or = |
# filter directly
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter |
descripFilter).show()

#filter through column
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter |descripFilter))\
.where("isExpensive")\
.select("unitPrice", "isExpensive").show(5)

# where may cause problems when use against null =>> null safe operation of "equalTo("str")"
df.where(col("Description").eqNullSafe("hello")).show()
from pyspark.sql.functions import window, column, desc, col
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
sc.setLogLevel("WARN")

spark = SparkSession.builder \
.master('local[*]') \
.appName('PySparkShell') \
.getOrCreate()

# Normal load
staticDataFrame = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Spark_the_def/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema
# # for x in staticSchema.fields:
# #     print (x.name)
#
# staticDataFrame\
# .selectExpr(
# "CustomerId",
# "(UnitPrice * Quantity) as total_cost",
# "InvoiceDate")\
# .groupBy(
# col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
# .sum("total_cost")\
# .show(5)

spark.conf.set("spark.sql.shuffle.partitions", "5")
# Stream load
streamingDataFrame = spark.readStream\
.schema(staticSchema)\
.option("maxFilesPerTrigger", 1)\
.format("csv")\
.option("header", "true")\
.load("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Spark_the_def/Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv")

purchaseByCustomerPerHour = streamingDataFrame\
.selectExpr(
"CustomerId",
"(UnitPrice * Quantity) as total_cost",
"InvoiceDate")\
.groupBy(
col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
.sum("total_cost")

purchaseByCustomerPerHour.writeStream\
.format("memory")\
.queryName("customer_purchases")\
.outputMode("complete")\
.start()

spark.sql("""
SELECT *
FROM customer_purchases
ORDER BY `sum(total_cost)` DESC
""")\
.show(5)
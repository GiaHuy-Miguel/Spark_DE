import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, StructType, StructField

# INIT DATA
spark = SparkSession.builder \
    .appName("Dat dep zai") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

data =[["11//02/2025"],
       ["27/11-2021"],
       ["28.12-2005"],
       ["14~9*2002"],
       ["-31:03{}1995"],
       ["2004/08/23"]]
df = spark.createDataFrame(data,["date"])
#df.show()

# TRANSFORMATION
def split_date(date: str) -> dict[str,str]|dict[str,None]:
    if date and isinstance(date, str):
        if date[0] == "-":
            date = date[1:]
            # print(date)
        date_list = re.split(r"//|{}|[~*{}.:/-]",date)
        if len(date_list[0])  > 2:
            return {"dayas": date_list[2], "monthas": date_list[1], "yearas": date_list[0]}
        else:
            return {"dayas": date_list[0], "monthas": date_list[1], "yearas": date_list[2]}
    return {"dayas": None, "monthas": None, "yearas": None}

# print(split_date("2004/08/23"))
dateUDF = udf(lambda x: split_date(x), StructType([
    StructField("dayas", StringType(), True),
    StructField("monthas", StringType(), True),
    StructField("yearas", StringType(), True),
]))

df.withColumn("date_split",dateUDF(col("date"))). \
    select(
        col("date"),
        col("date_split.dayas").alias("day"),
        col("date_split.monthas").alias("month"),
        col("date_split.yearas").alias("year"),
    ). \
    show()
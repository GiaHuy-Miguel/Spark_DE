from pyspark.sql.functions import col, length
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, \
    LongType, StringType,BooleanType

spark = SparkSession.builder \
    .appName("Huy dep zai") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

schema = StructType([
    StructField("id", StringType(),True),
    StructField("type", StringType(),True),
    StructField("actor", StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ])),
    StructField("repo", StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True)
    ]), True),
    StructField("payload", StructType([
        StructField("ref", StringType(), True),
        StructField("ref_type", StringType(), True),
        StructField("master_branch", StringType(), True),
        StructField("description", StringType(), True),
        StructField("pusher_type", StringType(), True),
    ])),
    StructField("public", BooleanType(), True),
    StructField("created_at", StringType(), True)
])

df = spark.read.\
    option("multiline", "true").\
    schema(schema).\
<<<<<<< HEAD
    json("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Course_anh_dat /Resources/large-file.json")
=======
    json("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Course_anh_dat /resources/large-file.json")
>>>>>>> 2edff36 (commit of 04/04/2025 - problem 1)

"""
    Lesson 
"""
# distinct =>> cho cả bảng
# df.select(col("payload.issue.state")).show()
df.select(col("payload.issue.state").alias("meomeo")).distinct().count("*").show()
     # .selectExpr("count(payload.issue.state")) =>>> không được bởi vì sau câu query bên trên,
     #  spark trả ra 1 DF ko có cột lồng

# drop_dup =>> cho từng cột chỉ định
df.select(col("payload.issue.state")).drop_duplicates(["state"]).show()
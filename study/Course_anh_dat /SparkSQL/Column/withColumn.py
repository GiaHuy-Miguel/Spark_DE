from pyspark.sql.connect.functions import length
from pyspark.sql.functions import col, upper, struct, lit, udf
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
# df.select(col("id")).sort(col("id")).show()
#
# df.select(col("id")).orderBy(col("ida").desc()).show()

# df.select(col("id"), col("actor.id")).orderBy([col("id"), col("actor.id")],ascending= [True, False]).show()

# =>> Bài toán sort hai cột ngược chiều nhau =>> WITH COLUMN
"""
withColumn: 
colName: 
- neu ton tai => ghi de 
- neu ko ton tia => them cot 
column: value of col
- them 1 cot moi neu colName h
"""
# withColumn => tạo cột với trong df
df.withColumn("id2", lit("hehehe")).select(col("id"),col("id2")).show()
df.withColumn("actor.id2", lit("hehehe")).select(col("actor.id"),col("actor.id2")).show()

#lit
df = df.withColumn(
    "actor",
    struct(
        col("actor.id").alias("id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        # lit(hehe("actor.id")).alias("id2")
        lit('5').alias("id2")
    )
)
df.show()





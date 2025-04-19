from pyspark.sql.functions import length
from pyspark.sql.functions import col, udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, \
    LongType, StringType, BooleanType, IntegerType, FloatType

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
    json("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Course_anh_dat /Resources/large-file.json")
"""
    Lesson: UDF - user define function 
    - là function/ các phép biến đổi mà user tạo trước 
    - khác với CTE chỉ lưu kết quả; UDF lưu logic biến đổi => sử dụng đi sử dụng lại (sẽ lưu vào libraries của DB)
    - Với pyspark, UDF áp dụng lên toàn bộ DF hoặc 1 cột của DF 
    - sử dụng bằng việc gọi method udf trong pyspark.sql.functions 
"""

def hehe(idx1, idx2): #idx
    buahehe =  (idx1 + idx2)/100000
    return buahehe

dzUDF = udf(lambda z,y: hehe(z,y), FloatType())

df.withColumn("hehe",dzUDF(col("actor.id"), col("id").cast(LongType()))) \
.orderBy([col("hehe"), col("id")],ascending= [False, True]) \
.select(col("id"), col("hehe")).show()
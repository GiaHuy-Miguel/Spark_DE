from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, \
    LongType, StringType,BooleanType

class JsonData4PySpark:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Huy dep zai") \
            .master("local[*]") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()

        self.schema = StructType([
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
    def get_df(self):
        return self.spark.read.\
            option("multiline", "true").\
            schema(self.schema).\
<<<<<<< HEAD
            json("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Course_anh_dat /Resources/large-file.json")
=======
            json("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Course_anh_dat /resources/large-file.json")
>>>>>>> 2edff36 (commit of 04/04/2025 - problem 1)

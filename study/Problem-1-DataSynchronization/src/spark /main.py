from config.database_config import get_dbconfig
from config.spark_config import SparkConnect
from spark_write_data import SparkConnectMySQL

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():

    JARS = ["/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/mysql-connector-j-9.2.0/mysql-connector-j-9.2.0.jar"]
    PATH = "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/data/2015-03-01-17.json"
    SCHEMA = StructType([
        StructField("actor", StructType([
            StructField("id", IntegerType(), True),
            StructField("login", StringType(), True),
            StructField("gravatar_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("avatar_url", StringType(), True)
        ]), True)
    ])

    PROPERTIES = {
        # Connection pooling and optimization properties
        "cachePrepStmts": "true",  # Cache prepared statements
        "prepStmtCacheSize": "250",  # Size of prepared statement cache
        "prepStmtCacheSqlLimit": "2048",  # Max SQL length for caching
        "useServerPrepStmts": "true",  # Use server-side prepared statements
        "rewriteBatchedStatements": "true",  # Optimize batch inserts
        "connectTimeout": "5000",  # Timeout for establishing connection
        "socketTimeout": "30000"  # Timeout for socket operations
    }

# CONNECT TO SPARK
    spark = SparkConnect(
        app_name="capuccina balerina",
        master_url="local[*]",
        # thực tế là đi làm ko được chạy trên local =>> phải link vào con master
        executor_memory='4g',
        executor_core=2,
        driver_memory='2g',
        numb_executors=3,
        jars=JARS,
        log_level= "WARN").spark_

# READ JSON FILE
    user_df = spark.read. \
        schema(SCHEMA). \
        json(PATH)
    user_df = user_df.select(col("actor.id").alias("users_id"),
                             col("actor.login"),
                             col("actor.gravatar_id"),
                             col("actor.url"),
                             col("actor.avatar_url")).distinct()
    user_df.show()

# UPLOAD TO DB
    db_config = get_dbconfig()
    spark_mysql = SparkConnectMySQL(spark,"GIT.Users" ,db_config)
    spark_mysql.write_mysql(user_df, PROPERTIES)
    spark_mysql.validate_import(user_df)

if __name__ == "__main__":
    main()
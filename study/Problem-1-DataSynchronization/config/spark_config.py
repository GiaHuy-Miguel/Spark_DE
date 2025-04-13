import os.path
from typing import Optional
from pyspark.sql import SparkSession
from config.database_config import get_dbconfig, DatabaseConfig

def create_spark_session(
        app_name: str,
        master_url: str = "local[*]",
        # thực tế là đi làm ko được chạy trên local =>> phải link vào con master
        executor_memory: Optional[str] = '4g',
        executor_core: int | None = 2,
        driver_memory: str| None = '2g',
        numb_executors: int | None = 3,
        jars: list[str] | None = None,
        spark_conf: dict[str, str]| None = None,
        log_level: str = "INFO") -> SparkSession: # Vào parameter -> ra SparkSession

        builder = SparkSession.builder \
            .appName(app_name) \
            .master(master_url)

        if executor_memory:
            builder.config("spark.executor.memory", executor_memory)
        if executor_core:
            builder.config("spark.executor.cores", executor_core)
        if driver_memory:
            builder.config("spark.driver.memory", driver_memory)
        if numb_executors:
            builder.config("spark.executor.instances", numb_executors)

        if jars:
            jars_path = ",".join([os.path.abspath(jar) for jar in jars ])

        # ['/home/miguel/mysql-connect-j-9.2.0/mysql-connect-j-9.2.0.jar',
        #  '/home/miguel/mysql-connect-j-9.2.0/redis-connect-j-9.2.0.jar',
        #  '/home/miguel/mysql-connect-j-9.2.0/mongodb-connect-j-9.2.0.jar']

        # =>>>>> ['/home/miguel/mysql-connect-j-9.2.0/mysql-connect-j-9.2.0.jar,
        #         /home/miguel/mysql-connect-j-9.2.0/redis-connect-j-9.2.0.jar,
        #         /home/miguel/mysql-connect-j-9.2.0/mongodb-connect-j-9.2.0.jar']
            builder.config("spark.jars", jars_path)
        if spark_conf:
            for k,v in spark_conf.items():
                builder.config(k,v)

        spark = builder.getOrCreate() # phải start spark trước mới sinh ra log để ghi

        spark.sparkContext.setLogLevel(log_level)

        return spark



def connect_mysql(spark_: SparkSession, config: dict[str, DatabaseConfig], table_name: str):
    # print(config["mongo"].uri)
    df = spark_.read \
        .format("jdbc") \
        .option("url", config["mysql"].url) \
        .option("dbtable", table_name) \
        .option("user", config["mysql"].user) \
        .option("password", config["mysql"].password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()
    return df


jars = [
    "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/lib/mysql-connector-j-9.2.0/mysql-connector-j-9.2.0.jar"]

spark = create_spark_session(
    app_name="capuccina balerina",
    master_url="local[*]",
    # thực tế là đi làm ko được chạy trên local =>> phải link vào con master
    executor_memory='4g',
    executor_core=2,
    driver_memory='2g',
    numb_executors=3,
    jars=jars,
    log_level="WARN"
)

DB_CONFIG = get_dbconfig()
MYSQL_TABLE_NAME = "Repo"

df = connect_mysql(spark, DB_CONFIG, MYSQL_TABLE_NAME)
print("-----------------------------SPARK HAS CONNECTED TO MYSQL SUCCESSFULLY--------------")
# df.printSchema()

# data =[["quan", 18],["minh", 16],["dat", 10]]
#
# spark.createDataFrame(data, ["ten", "tuoi"]).show()




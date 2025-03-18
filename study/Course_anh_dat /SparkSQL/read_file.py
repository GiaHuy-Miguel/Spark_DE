from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Dat dep zai") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# csv_file = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Spark_the_def/Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv")
# csv_file.show()

json_file = spark.read.json("")
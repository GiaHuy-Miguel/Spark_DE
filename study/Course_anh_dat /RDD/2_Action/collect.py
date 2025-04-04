from pyspark import SparkContext, SparkConf

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]") \
        .set("spark.executor.pyspark.memory", "4g")
    # setMaster("local[8]").\ =>> chạy với 8 threads/core cpu/ số partitions. "*" là full threads

sc = SparkContext(conf=conf)

fileRDD = sc.textFile("/home/miguel/HUY/Work/COURSE/DE_DA/DE_Anh Dat/Spark/study/Spark_the_def/Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv")

allCapRDD = fileRDD.map(lambda text: text.upper())
print(allCapRDD.collect())
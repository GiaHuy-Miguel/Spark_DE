from pyspark import SparkContext, SparkConf

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]")
    #   .set("spark.executor.pyspark.memory", "4g")
    # setMaster("local[8]").\ =>> chạy với 8 threads/core cpu/ số partitions. "*" là full threads

sc = SparkContext(conf=conf)

data = sc.parallelize([(110, 58.9), (123, 80.0),
                       (129, 75.5), (791, 90)])
data1 = sc.parallelize([(110, "huy"), (123, "tien"), (791, "dat"), (911, "cong an")])

print(data1.lookup(110))

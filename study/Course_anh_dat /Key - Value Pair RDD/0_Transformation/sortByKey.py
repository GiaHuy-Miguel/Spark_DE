from pyspark import SparkContext, SparkConf

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]")
    #   .set("spark.executor.pyspark.memory", "4g")
    # setMaster("local[8]").\ =>> chạy với 8 threads/core cpu/ số partitions. "*" là full threads

sc = SparkContext(conf=conf)

data = sc.parallelize([("dat-debt", 5.0), ("tien-debt", 1.3),
                       ("huy-debt", 7.0), ("tien-debt", 8.1),
                       ("quanh-debt", 15.8)])

rev_as_key = data.map(lambda x: (x[1], x[0])) # Tối ưu nếu viết với Java/Scala : v -> (v._2, v._1)
print(rev_as_key.collect())

sortd = rev_as_key.sortByKey(False) # ASC = False
print(sortd.take(1))
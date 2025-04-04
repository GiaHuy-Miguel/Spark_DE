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

total_outstanding = data.map(lambda x: x[1]).reduce(lambda v1,v2: v1+v2)
print(f"total outstanding: {total_outstanding}")

bill = data.reduceByKey(lambda key,value: key + value)
print(bill.collect())

re = data.map(lambda x: (x[1], x[0]))


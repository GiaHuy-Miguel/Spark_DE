from pyspark import SparkContext, SparkConf

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]")
    #   .set("spark.executor.pyspark.memory", "4g")
    # setMaster("local[8]").\ =>> chạy với 8 threads/core cpu/ số partitions. "*" là full threads

sc = SparkContext(conf=conf)

rdd1 = sc.parallelize(["meo meo meo meo, toi la con meo"])
rdd2 = rdd1.flatMap(lambda x: x.split(" ")).map(lambda v:(len(v), v))
grouped_rdd = rdd2.groupByKey()

for keys in grouped_rdd.collect():
    print(keys)
print()

for key, values in grouped_rdd.collect():
    print(f"({key}, {list(values)})")



from pyspark import SparkContext, SparkConf

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]") \

sc = SparkContext(conf=conf)
sc.setLogLevel(logLevel="WARN")

RDD1 = sc.parallelize([1,2,4,568,9,2,435,212])
RDD2 = sc.parallelize([21,213,5,345,2312,1346,88])

RDD_unified = RDD2.union(RDD1)
print(RDD1.collect())
print(RDD2.collect())
print(RDD_unified.collect())
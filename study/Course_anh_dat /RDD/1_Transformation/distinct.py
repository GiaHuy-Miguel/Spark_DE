from pyspark import SparkContext, SparkConf

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]") \

sc = SparkContext(conf=conf)
sc.setLogLevel(logLevel="WARN")

RDD1 = sc.parallelize([1,2,4,9,5,4,5,5,56,6,9,2,3,4,3,5,212])
print(RDD1.distinct().collect())
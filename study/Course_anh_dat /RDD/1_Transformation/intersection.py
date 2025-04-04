from pyspark import SparkContext, SparkConf

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]") \

sc = SparkContext(conf=conf)
sc.setLogLevel(logLevel="WARN")

RDD1 = sc.parallelize([1,2,4,568,9,2,435,212])
RDD2 = sc.parallelize([2,1,212,5,345,2312,1346,88])

intersection = RDD1.intersection(RDD2)
print(intersection.collect())
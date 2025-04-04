from pyspark import SparkContext, SparkConf

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]") \
    #   .set("spark.executor.pyspark.memory", "4g")
    # setMaster("local[8]").\ =>> chạy với 8 threads/core cpu/ số partitions. "*" là full threads

sc = SparkContext(conf=conf)
def sum(v1: int, v2: int) -> int:
    # print(f"(v1: {v1}, v2: {v2}) => {v1+v2}")
    return v1 + v2
numsRDD = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 3)

total = numsRDD.reduce(sum)
print(total)

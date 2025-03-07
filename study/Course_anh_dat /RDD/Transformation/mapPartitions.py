import time
from pyspark import SparkContext, SparkConf
from random import Random

def nums_partition(iterator):
    rand = Random(int(time.time()*1000) + Random().randint(0,1000))
    return {f"{item}:{rand.randint(0,1000)}" for item in iterator}

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]") \
        .set("spark.executor.pyspark.memory", "4g")
    # setMaster("local[8]").\ =>> chạy với 8 threads/core cpu/ số partitions. "*" là full threads

sc = SparkContext(conf=conf)
data = ["Huy", "Hanh", "Manh", "Thoi"]
initRDD = sc.parallelize(data)

mapPartitionRDD1 = initRDD.mapPartitions(nums_partition) # Map vào cả partition => ánh xạ (map) iterator với iterator
mapPartitionRDD2 = initRDD.mapPartitions(
    lambda item: map(
        lambda l: f"{l}: {(Random(int(time.time()*1000) + Random().randint(0,1000))).randint(0,1000)}", item
    )
)
# print(mapPartitionRDD1.collect())
print(mapPartitionRDD2.collect())

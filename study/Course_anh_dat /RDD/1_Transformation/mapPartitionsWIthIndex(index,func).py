from pyspark import SparkContext, SparkConf

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]") \

sc = SparkContext(conf=conf)
sc.setLogLevel(logLevel="WARN")

numbersRDD = sc.parallelize([1,2,3,4,5,7,9,0,10,22], 5)
# numbersRDD = sc.parallelize([1,2,3,4,5,7,9,0,10,22], 2)
# =>> (1,0),(2,0),(3,0),(4,0),(5,0)/ (7,1),(9,1),(0,1),(10,1),(22,1)
"""
    idx: id of partition (e.g., 0, 1, 2, etc.)
    itr: iterate through all data in the partitions
    =>> n: with every data in the partition, the func result (n, idx)
    
    With the given idx, you can apply each partition with particular function individually
    But it is rarely used
"""

result = (numbersRDD.mapPartitionsWithIndex(
    lambda idx, itr: [(n, idx) for n in itr]
)
.collect())

print(result)
print(numbersRDD.glom().collect())
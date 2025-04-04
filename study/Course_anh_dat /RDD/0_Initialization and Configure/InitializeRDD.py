from pyspark import SparkContext

#create sparkContext
sc = SparkContext("local", "Huydz")

#create object
data = [
    {"id": 1, "name": "dat"},
    {"id": 2, "name": "heu kkk"},
    {"id": 3, "name": "tuan minh"},
    {"id": 4, "name": "huy"},
    {"id": 5, "name": "manh"},
    {"id": 6, "name": "my"},
]
# print(data)

# create rdd from data
rdd = sc.parallelize(data)
print(rdd.collect()) # print list formate
print(f"the number of obs: {rdd.count()}") # count the number of data in a rdd
print(f"first value of data: {rdd.first()}") # print first value of the rdd

from pyspark import SparkContext, SparkConf
# Create Spark Context & Conf
conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]") \
        .set("spark.executor.pyspark.memory", "4g")
    # setMaster("local[8]").\

sc = SparkContext(conf=conf)

# Initial RDD
input_data = [1,2,4,6,7,3,6,7,8,9]
initRDD = sc.parallelize(input_data)
print(initRDD.getNumPartitions())

# 1_Transformation
squareRDD = initRDD.map(lambda i: i*i)

filterRDD = initRDD.filter(lambda i: i>4) # lấy những số lớn hơn 4
# filterRDD.foreach(lambda i: print(i))

flatMapRDD = initRDD.flatMap(lambda i: [i, i*2]) # Từ RDD gốc, cho 1 RDD mới có nhiều dữ kiện hơn
print(flatMapRDD.collect())
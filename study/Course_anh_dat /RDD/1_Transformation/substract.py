from pyspark import SparkContext, SparkConf

conf = SparkConf() \
        .setAppName("meo") \
        .setMaster("local[*]") \

sc = SparkContext(conf=conf)
sc.setLogLevel(logLevel="WARN")

text = sc.parallelize(["DIT CON me MAyyyyy ahhhhhh"]) \
    .flatMap(lambda  l: l.split(" ")) \
    .map(lambda w: w.lower())

remove_text = sc.parallelize(["dit con me mayyyyy"]) \
    .flatMap(lambda x: x.split())

censor = text.subtract(remove_text)

print(text.collect())
print(remove_text.collect())
print(censor.collect())
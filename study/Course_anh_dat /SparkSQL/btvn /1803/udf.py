import data
import random


from pyspark.sql.functions import col, udf
from pyspark.sql.types import \
    StringType

init = data.JsonData4PySpark()
df = init.get_df()

"""
    Lesson: Bài toán sort hai cột ngược chiều nhau =>> WITH COLUMN + UDF 
"""

def hehe(idx):
     muahehe = random.randint(1, 10000)*5 + 22
     return muahehe
# biến idx là cả 1 DF hay chỉ là 1 cột trong DF thôi ?

dzUDF = udf(lambda z: hehe(z), StringType())
df.withColumn("hehe",dzUDF(col("actor.id"))) \
.orderBy([col("hehe"), col("id")],ascending= [False, True]) \
.select(col("hehe"), col("id")). show()



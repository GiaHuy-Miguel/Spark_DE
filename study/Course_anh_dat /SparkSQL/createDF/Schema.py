from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, \
    LongType, StringType, IntegerType, \
    FloatType, DoubleType, ArrayType, BooleanType, DateType, TimestampType, MapType

spark = SparkSession.builder \
    .appName("Dat dep zai") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
#
# data = spark.sparkContext.parallelize([
#     Row(1,"dat",18),
#     Row(2,"hieu",24),
#     Row(3,"huy",17),
#     Row(None, None, None)
#     ])
#
# schema = StructType([
#     StructField("id", LongType(), True),
#     StructField("name", StringType(), True),
#     StructField("age", LongType(), False) #nullable = True => cho phép có giá trị null
# ])
#
# df = spark.createDataFrame(data,schema).show()

"""
PYSPARK SQL TYPE
StringType: chuoi ky tu
Long Type: so nguyen 64 bit 
Integer Type: so nguyen 32 bit 
ByteType: so nguyen 8 bit 
ShortType: so nguyen 16 bit 
FloatType: so thap phan 32 bit 
DoubleType: so thap phan 64 bit 
TimestampType: ngay va gio 
DateType 
DecimalType(precision, scale): do chinh xac cua so thap phan
    - precision: tong chu so
    - scale: so chu so sau dau phay
=================ADVANCE=========================
StructType: bieu dien mot cau truc 
StructField(name, datatype, nullable): bieu dien 1 truong trong StrucType
ArrayType(elementType): bieu dien cac mang duoc chi dinh 
    - elementType: kieu du lieu cua cac phan tu trong mang 
MapType(keyType, valueType): bieu dien cap khoa key-value
    - keyType: kieu du lieu cua "key" 
    - valueType: kieu du lieu cua "value"
"""

data = [
    Row(
        name = "Quang Anh Tran",
        age = 15,
        id = 1,
        salary = 100000.0,
        bonus = 5000.75,
        scores = [1,8,9],
        is_active = True,
        attributes = {"dept":"Engineering", "role": "DE"},
        hire_date = datetime.strptime("2024-1-14", "%Y-%m-%d").date(),
        last_login = datetime.strptime("2025-3-14 22:15:14", "%Y-%m-%d %H:%M:%S"),
        tax_rate = 12.34
        ),
    Row(
        name = "Le Bao Hoang",
        age = 25,
        id = 2,
        salary = 20000.0,
        bonus = 1000.75,
        scores = [8,9,9],
        is_active = False,
        attributes = {"dept":"Massage", "role": "BJ"},
        hire_date = datetime.strptime("2023-7-29", "%Y-%m-%d").date(),
        last_login = datetime.strptime("2025-3-12 10:20:45", "%Y-%m-%d %H:%M:%S"),
        tax_rate = 20.63
        )
]

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("id", LongType(), True),
    StructField("salary", FloatType(), True),
    StructField("bonus", DoubleType(), True),
    StructField("scores", ArrayType(IntegerType(), True), True),
    StructField("is_active", BooleanType(), True),
    StructField("attributes", MapType(StringType(), StringType(), True), True),
    StructField("hire_date", DateType(), True),
    StructField("last_login", TimestampType(), True),
    StructField("tax_rate", DoubleType(), True),
])
df = spark.createDataFrame(data, schema).show(truncate=False)
print(schema)

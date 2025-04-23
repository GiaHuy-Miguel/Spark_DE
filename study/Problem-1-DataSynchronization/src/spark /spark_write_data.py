from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from config.database_config import DatabaseConfig


class SparkConnectMySQL:
    def __init__(self, spark: SparkSession, table_name: str, db_config: dict[str,DatabaseConfig]):
        self.spark = spark
        self.db_config = db_config
        self.spark_ = spark
        self.table_name = table_name

    def read_mysql(self):
        df = self.spark_.read \
            .format("jdbc") \
            .option("url", self.db_config["mysql"].url) \
            .option("dbtable", self.table_name) \
            .option("user", self.db_config["mysql"].user) \
            .option("password", self.db_config["mysql"].password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
        return df

    def write_mysql(self, df: DataFrame, properties):
        # df.write \
        # .format("jdbc") \
        # .option("url", self.db_config["mysql"].url) \
        # .option("dbtable", table_name) \
        # .option("user", self.db_config["mysql"].user) \
        # .option("password", self.db_config["mysql"].password) \
        # .option("driver", "com.mysql.cj.jdbc.Driver") \
        # .mode("append") \
        # .save()
        properties.update({"user": self.db_config["mysql"].user,
                           "password": self.db_config["mysql"].password,
                           "driver": "com.mysql.cj.jdbc.Driver"})
        # print(properties)
        df.write.jdbc(
            url= self.db_config["mysql"].url,
            table= self.table_name,
            mode="append",
            properties=properties
        )
        print("----------------Database Written-----------------")

    def validate_import(self, data_source: DataFrame):
        db_data = self.read_mysql()
        check1 = db_data.exceptAll(data_source)
        check2 = data_source.exceptAll(db_data)
        if not check1.isEmpty():
            raise Exception("-------------------------Import Failure: Have Not Deleted Test Data-----------------")
        if not check2.isEmpty():
            raise Exception("-------------------------Import Failure: Missing Data-----------------")
        print("--------------------Import Validated----------------------")

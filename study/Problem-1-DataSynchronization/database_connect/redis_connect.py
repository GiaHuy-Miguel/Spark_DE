import redis
import json

from redis.connection import ConnectionError

class RedisConnect:
    def __init__(self, host:str, port: int, password: str, database: int, user: str):
        self.host = host
        self.port = port
        self.password = password
        self.database = database
        self.username = user
        self.client = None

    def connect(self):
        try:
            self.client = redis.StrictRedis(
                host= self.host,
                port= self.port,
                password=self.password,
                db= self.database,
                username=self.username,
                decode_responses=True)
        except ConnectionError as e:
            raise Exception(f"---------------Failed to Connect Redis: {e}-------------")
        return self.client

    def disconnect(self):
        self.client.close()

    def __enter__(self):
        self.connect()
        return  self.client.ping()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.disconnect()

    def write_data(self, json_path: str|None):
        if json_path:
            with open(json_path, 'r') as file:
                data = json.load(file)
                # print(data)
        if data["actor"]:
            self.client.hset("1843574", mapping=data["actor"])
            print("------------------Data Writen - Key: 1843574 ----------------------")
        else:
            print("---------------------Error: Cannot find data-------------------")

    def get_data(self, key_):
        data = self.client.hgetall(f"{key_}")
        print(data)
        return data


# PATH = "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/data/sample.json"
#
# redis_config = get_dbconfig()["redis"].__dict__
# # print(redis_config)
# r = connect_to_redis(**redis_config)
#
# create_redis_schema(PATH, r)
# print(get_redis_data(1843574, r))

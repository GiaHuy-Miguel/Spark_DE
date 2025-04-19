import redis
import json

from redis import StrictRedis
from redis.connection import ConnectionError
from config.database_config import get_dbconfig


def connect_to_redis(host:str, port: int, password: str, database: int):
    try:
        redis_ = redis.StrictRedis(
            host=host,
            port=port,
            # username=user,
            password=password,
            db=database,
            decode_responses=True)
        print("----------------Redis Connected Successfully-----------------")
    except ConnectionError as e:
        raise Exception(f"---------------Failed to Connect Redis: {e}-------------")
    return redis_

def create_redis_schema(json_path: str|None, redis_client: StrictRedis):
    if json_path:
        with open(json_path, 'r') as file:
            data = json.load(file)
            # print(data)
    if data["actor"]:
        redis_client.hset("1843574", mapping=data["actor"])
        print("------------------Data Writen----------------------")
    else:
        print("---------------------Error: Cannot find data-------------------")
def get_redis_data(id, redis_client: StrictRedis):
    return redis_client.hgetall(f"{id}")


# PATH = "/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/data/sample.json"
#
# redis_config = get_dbconfig()["redis"].__dict__
# # print(redis_config)
# r = connect_to_redis(**redis_config)
#
# create_redis_schema(PATH, r)
# print(get_redis_data(1843574, r))

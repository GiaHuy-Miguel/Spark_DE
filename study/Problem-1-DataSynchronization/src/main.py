from pathlib import Path
from bson import Int64

from config.database_config import get_dbconfig

from database_connect.mongo_connect import MongoConnect
from database_connect.mysql_connect import MySQLConnect
from database_connect.redis_connect import connect_to_redis

from database_connect.schema_manager import create_mongo_schema, validate_mongo_schema, create_mysql_schema, \
    validate_mysql_schema

DATABASE_NAME = "GIT"
SQL_FILE_PATH = Path("/home/miguel/HUY/STUDY/COURSE/Spark_Python/Spark_DE/study/Problem-1-DataSynchronization/database_connect/mysql_schema.sql")

def main():

# CONNECT TO MYSQL
    db_configs = get_dbconfig()["mysql"].__dict__
    initial_config = {k: v for k, v in db_configs.items() if k not in ("database", "url", "driver")}
    # print(initial_config)
    try:
        with MySQLConnect(**initial_config) as mysql_client:
            connection, cursor = mysql_client.connect()
    except Exception as e:
        print(f"------------------Error: {e}------------------")
        if connection and connection.is_connected():
            connection.rollback()
    connection.database = DATABASE_NAME
    create_mysql_schema(cursor, DATABASE_NAME, SQL_FILE_PATH)

    cursor.execute("SELECT * FROM Users WHERE users_id = 1")
    user_check = cursor.fetchone()
    if not user_check:
        cursor.execute("INSERT INTO Users  (users_id, login, gravatar_id, url, avatar_url) VALUES (%s, %s, %s, %s, %s)",
                       (1, "jsonmurphy", "", "https://api.github.com/users/jsonmurphy", "https://avatars.githubusercontent.com/u/1843574?"))
    connection.commit()
    validate_mysql_schema(cursor)

# CONNECT TO MONGODB
    config_mongo = get_dbconfig()

    mongodb_client = MongoConnect(config_mongo["mongo"].uri, config_mongo["mongo"].db_name)
    create_mongo_schema("Users",mongodb_client.connect())

    # Insert sample record
    mongodb_client.db.Users.insert_one({
        "user_id": Int64(1),
        "login": "jsonmurphy",
        "gravatar_id": "",
        "url": "https://api.github.com/users/jsonmurphy",
        "avatar_url": "https://avatars.githubusercontent.com/u/1843574?"
    })
    print("-------------------Inserted Record to MongoDB-------------------------------")

    validate_mongo_schema("Users",mongodb_client.connect())

 # CONNECT TO REDIS
    redis_config = get_dbconfig()["redis"].__dict__
    connect_to_redis(**redis_config)

if __name__ == "__main__":
    main()
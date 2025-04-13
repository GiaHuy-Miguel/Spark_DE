from bson import Int64

from config.database_config import get_dbconfig
from database_connect.mongo_connect import MongoConnect
from database_connect.schema_manager import create_mongo_schema, validate_mongo_schema


def main():
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

if __name__ == "__main__":
    main()
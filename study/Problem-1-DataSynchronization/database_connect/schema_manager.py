from collections.abc import Mapping
from bson.int64 import Int64

from pymongo.synchronous.database import Database


def create_mongo_schema(collection_name:str, db: Database[Mapping[str]]):
    if collection_name in db.list_collection_names():
        raise ValueError("-----------------Collection Exists------------------")
    else:
        db.create_collection(collection_name,validator={
            "$jsonSchema":{
                "bsonType": "object",
                "required": ["user_id", "login"],
                "properties": {
                    "user_id": {
                        "bsonType": "long"
                    },
                    "login": {
                        "bsonType": "string"
                    },
                    "gravatar_id": {
                        "bsonType": ["string", "null"]
                    },
                    "avatar_url": {
                        "bsonType" : ["string", "null"]
                    },
                    "url": {
                        "bsonType": ["string", "null"]
                    }
                }
            }
        })

def validate_mongo_schema(collection_name:str, db):
    collections = db.list_collection_names()
    # print (collections)
    if collection_name not in collections:
        raise ValueError("---------------------Missing Collection in DB-----------------------")
    user = db.Users.find_one({"user_id": Int64(1)})
    if not user:
        raise ValueError("---------------------Missing Value in DB-----------------------")
    print("--------------------------Mongo DB Schema Validated----------------------")
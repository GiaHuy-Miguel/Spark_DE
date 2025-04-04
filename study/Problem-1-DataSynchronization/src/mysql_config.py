from dotenv import load_dotenv
import os
from urllib.parse import urlparse

def get_config():
    load_dotenv()
    # print(load_dotenv())

    jdbc_url = os.getenv("DB_URL")

    parser_url = urlparse(jdbc_url.replace("jdbc:","",1))
    host = parser_url.hostname
    port = parser_url.port
    database = parser_url.path.strip("/")
    # print((host, port, database))

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    return {
        "host": host,
        "port": port,
        "user" : user,
        "password": password,
        "database": database
    }
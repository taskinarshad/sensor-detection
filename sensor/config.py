import pymongo
import pandas as pd
import json
from dataclasses import dataclass
import os
# Provide the mongodb localhost url to connect python to mongodb.


@dataclass
class EnvironmentVariable:
    mongo_db_url:str = os.getenv("MONGO_DB_URL")


TARGET_COLUMN_MAPPING ={
    "pos":1,
    "neg":0
}
    

env_var = EnvironmentVariable()
mongo_client = pymongo.MongoClient(env_var.mongo_db_url)
TARGET_COLUMN = "class"

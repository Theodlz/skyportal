import datetime
import traceback

import requests
from marshmallow.exceptions import ValidationError
from pymongo import MongoClient
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

from baselayer.app.access import auth_or_token, permissions
from baselayer.app.env import load_env
from baselayer.log import make_log

from ...models import Filter
from ..base import BaseHandler

log = make_log("api/boom_filter_modules")

_, cfg = load_env()


def get_db_uri():
    try:
        return f"{cfg['boom.filter_modules.mongodb_uri']}"
    except Exception as e:
        log(f"Error getting DB URI: {e}")
        return None


def get_db_name():
    try:
        return cfg["boom.filter_modules.database"]
    except Exception as e:
        log(f"Error getting DB name: {e}")
        return None


uri = get_db_uri()
queryDbName = get_db_name()


class BoomFilterModulesHandler(BaseHandler):
    @auth_or_token
    def get(self, name=None):
        elements = self.get_query_argument("elements")

        with self.Session() as session:
            client = MongoClient(uri)
            try:
                db = client[queryDbName]
                collection = db[elements]
                if name is None:
                    result = list(collection.find())
                elif elements == "schema":
                    result = collection.find_one({"instrument_name": name})
                else:
                    result = collection.find_one({"name": name})
            except Exception as e:
                traceback.print_exc()
                return self.error(f"Error fetching data from MongoDB: {e}")
            finally:
                client.close()
                
        return self.success(data={str(elements): result})

    @auth_or_token
    def post(self, name):
        # Handle POST requests for boom filter modules
        data = self.get_json()

        with self.Session() as session:
            client = MongoClient(uri)

            try:
                db = client[queryDbName]
                collection = db[data["elements"]]
                if data["elements"] == "blocks":
                    result = collection.insert_one(
                        {
                            "name": name,
                            "block": data["data"]["block"],
                            "created_at": datetime.datetime.utcnow(),
                        }
                    )
                elif data["elements"] == "variables":
                    result = collection.insert_one(
                        {
                            "name": name,
                            "variable": data["data"]["variable"],
                            "type": data["data"]["type"],
                            "created_at": datetime.datetime.utcnow(),
                        }
                    )
                elif data["elements"] == "listVariables":
                    result = collection.insert_one(
                        {
                            "name": name,
                            "listCondition": data["data"]["listCondition"],
                            "type": data["data"]["type"],
                            "created_at": datetime.datetime.utcnow(),
                        }
                    )
            except Exception as e:
                traceback.print_exc()
                return self.error(f"Error inserting data into MongoDB: {e}")
            finally:
                client.close()

        return self.success()

import traceback
from datetime import datetime, timedelta

import requests
from pymongo import MongoClient

from baselayer.app.access import auth_or_token
from baselayer.app.env import load_env
from baselayer.log import make_log

from ...base import BaseHandler
from .utils import boom_available, boom_token, boom_url

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
    @boom_available
    def get(self):
        elements = self.get_query_argument("elements")
        survey = self.get_query_argument("survey", None)

        with self.Session() as session:
            client = MongoClient(uri)
            try:
                db = client[queryDbName]
                collection = db[elements]
                if survey is None:
                    result = list(collection.find())
                elif elements == "schema":
                    result = requests.get(
                        f"{boom_url}/filters/schemas/{survey}",
                        headers={"Authorization": f"Bearer {boom_token}"},
                    )
                    if result.status_code != 200:
                        return self.error(
                            f"Boom API error: {result.status_code} - {result.text}"
                        )
                    try:
                        result = result.json()["data"]
                    except (KeyError, ValueError) as e:
                        return self.error(
                            f"Invalid JSON response from Boom API: {result.text}"
                        )
                else:
                    result = collection.find_one({"name": survey})
            except Exception as e:
                traceback.print_exc()
                return self.error(f"Error fetching data from MongoDB: {e}")
            finally:
                client.close()

        return self.success(data={str(elements): result})

    @auth_or_token
    @boom_available
    def post(self, name):
        # Handle POST requests for boom filter modules
        data = self.get_json()

        with self.Session() as session:
            client = MongoClient(uri)

            try:
                db = client[queryDbName]
                collection = db[data["elements"]]
                if data["elements"] == "blocks":
                    streams = data["data"].get("streams", [])
                    # Extract stream name (first part before space) from each stream
                    streams = [s.split(" ")[0] for s in streams] if streams else []
                    result = collection.insert_one(
                        {
                            "name": name,
                            "block": data["data"]["block"],
                            "streams": streams,
                            "created_at": datetime.utcnow(),
                        }
                    )
                elif data["elements"] == "variables":
                    streams = data["data"].get("streams", [])
                    # Extract stream name (first part before space) from each stream
                    streams = [s.split(" ")[0] for s in streams] if streams else []
                    result = collection.insert_one(
                        {
                            "name": name,
                            "variable": data["data"]["variable"],
                            "type": data["data"]["type"],
                            "streams": streams,
                            "created_at": datetime.utcnow(),
                        }
                    )
                elif data["elements"] == "listVariables":
                    streams = data["data"].get("streams", [])
                    # Extract stream name (first part before space) from each stream
                    streams = [s.split(" ")[0] for s in streams] if streams else []
                    result = collection.insert_one(
                        {
                            "name": name,
                            "listCondition": data["data"]["listCondition"],
                            "type": data["data"]["type"],
                            "streams": streams,
                            "created_at": datetime.utcnow(),
                        }
                    )
                elif data["elements"] == "switchCases":
                    streams = data["data"].get("streams", [])
                    # Extract stream name (first part before space) from each stream
                    streams = [s.split(" ")[0] for s in streams] if streams else []
                    result = collection.insert_one(
                        {
                            "name": name,
                            "switchCondition": data["data"]["switchCondition"],
                            "type": data["data"]["type"],
                            "streams": streams,
                            "created_at": datetime.utcnow(),
                        }
                    )
            except Exception as e:
                traceback.print_exc()
                return self.error(f"Error inserting data into MongoDB: {e}")
            finally:
                client.close()

        return self.success()

    @auth_or_token
    @boom_available
    def put(self, name):
        # Handle PUT requests for updating boom filter modules
        data = self.get_json()

        with self.Session() as session:
            client = MongoClient(uri)

            try:
                db = client[queryDbName]
                collection = db[data["elements"]]

                update_data = {"updated_at": datetime.utcnow()}

                if data["elements"] == "blocks":
                    update_data["block"] = data["data"]["block"]
                elif data["elements"] == "variables":
                    update_data["variable"] = data["data"]["variable"]
                    update_data["type"] = data["data"]["type"]
                elif data["elements"] == "listVariables":
                    update_data["listCondition"] = data["data"]["listCondition"]
                    update_data["type"] = data["data"]["type"]
                elif data["elements"] == "switchCases":
                    update_data["switchCondition"] = data["data"]["switchCondition"]
                    update_data["type"] = data["data"]["type"]

                result = collection.update_one({"name": name}, {"$set": update_data})

                if result.matched_count == 0:
                    return self.error(f"No document found with name: {name}")

            except Exception as e:
                traceback.print_exc()
                return self.error(f"Error updating data in MongoDB: {e}")
            finally:
                client.close()

        return self.success()

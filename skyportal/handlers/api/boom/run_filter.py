# from pymongo import MongoClient
from datetime import datetime, timedelta

import requests

from baselayer.app.env import load_env
from baselayer.log import make_log

from ....models import Filter
from ...base import BaseHandler
from .utils import boom_available, boom_token, boom_url, convert_large_ints

log = make_log("app/boom-run-filter")


class BoomRunFilterHandler(BaseHandler):
    @boom_available
    def post(self):
        data = self.get_json()
        with self.Session() as session:
            f = session.scalar(
                Filter.select(session.user_or_token, mode="update").where(
                    Filter.id == data["filter_id"]
                )
            )
            if "sort_by" not in data:
                data_url = f"{boom_url}/filters/test/count"
                data_payload = {
                    "permissions": {
                        data["selectedCollection"].split("_")[0]: f.stream.altdata[
                            "selector"
                        ]
                    },
                    "survey": data["selectedCollection"].split("_")[0],
                    "pipeline": data["pipeline"],
                    "start_jd": data["start_jd"],
                    "end_jd": data["end_jd"],
                }
            else:
                data_url = f"{boom_url}/filters/test"
                data_payload = {
                    "permissions": {
                        data["selectedCollection"].split("_")[0]: f.stream.altdata[
                            "selector"
                        ]
                    },
                    "survey": data["selectedCollection"].split("_")[0],
                    "pipeline": data["pipeline"],
                    "start_jd": data["start_jd"],
                    "end_jd": data["end_jd"],
                    "sort_by": data["sort_by"],
                    "sort_order": data["sort_order"],
                    "limit": data["limit"],
                }
                if "cursor" in data and data["cursor"] is not None:
                    data["cursor"] = int(data["cursor"])
                    if data["sort_order"] == "Ascending":
                        cursor_condition = {"$gt": int(data["cursor"])}
                    else:
                        cursor_condition = {"$lt": int(data["cursor"])}
                    data_payload["pipeline"].insert(
                        len(data_payload["pipeline"]) - 1,
                        {"$match": {"_id": cursor_condition}},
                    )

            headers = {
                "Authorization": f"Bearer {boom_token}",
                "Content-Type": "application/json",
            }

            response = requests.post(data_url, json=data_payload, headers=headers)

            if response.status_code != 200:
                return self.error(
                    f"Error querying Boom: {response.status_code} {response.text}"
                )
            res = response.json()
            if "sort_by" in data:
                res["data"]["results"] = [
                    {**doc, "_id": str(doc["_id"])} for doc in res["data"]["results"]
                ]
            res = convert_large_ints(res)
        return self.success(data=res)

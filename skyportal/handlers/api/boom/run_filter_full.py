from datetime import datetime, timedelta

import requests

from baselayer.app.env import load_env
from baselayer.log import make_log

from ....models import Filter
from ...base import BaseHandler
from .utils import convert_large_ints

log = make_log("app/boom-run-filter-full")

_, cfg = load_env()


def get_boom_url():
    try:
        ports_to_ignore = [443, 80]
        return f"{cfg['boom.protocol']}://{cfg['boom.host']}" + (
            f":{int(cfg['boom.port'])}"
            if (
                isinstance(cfg["boom.port"], int)
                and int(cfg["boom.port"]) not in ports_to_ignore
            )
            else ""
        )
    except Exception as e:
        log(f"Error getting Boom URL: {e}")
        return None


def get_boom_credentials():
    username = cfg["boom.username"]
    password = cfg["boom.password"]
    return {"username": username, "password": password}


boom_url = get_boom_url()
boom_credentials = get_boom_credentials()


def get_boom_token():
    try:
        if boom_url is None:
            return None, None
        auth_url = f"{boom_url}/auth"
        current_time = datetime.utcnow()
        auth_response = requests.post(
            auth_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=boom_credentials,
        )
        auth_response.raise_for_status()
        data = auth_response.json()
        token = data["access_token"]
        expires_at = None
        if data.get("expires_in"):
            expires_in = int(data["expires_in"])
            expires_at = current_time + timedelta(seconds=expires_in)
        return token, expires_at
    except Exception as e:
        log(f"Error getting Boom token: {e}")
        return None, None


boom_token, boom_token_expires_at = get_boom_token()


def boom_available(func):
    def wrapper(*args, **kwargs):
        global boom_url
        global boom_credentials
        if boom_url is None or boom_credentials is None:
            raise ValueError("Boom is not available")
        global boom_token
        global boom_token_expires_at
        if boom_token is None or (
            boom_token_expires_at is not None
            and boom_token_expires_at < datetime.utcnow() + timedelta(seconds=1800)
        ):
            boom_token, boom_token_expires_at = get_boom_token()
        if boom_token is None:
            raise ValueError("Boom is not available")
        return func(*args, **kwargs)

    return wrapper


class BoomRunFilterFullHandler(BaseHandler):
    """Fetch full alert documents by primary-key IDs.

    The frontend handles ID collection by reusing the existing /run_filter
    endpoint (which already pages through /filters/test).  This handler only
    does the second phase: given a batch of _id integers it issues a simple
    ``{$match: {_id: {$in: ids}}}`` against Boom's /queries/pipeline endpoint,
    which returns complete alert documents without requiring a $project stage.
    """

    @boom_available
    def post(self):
        data = self.get_json()
        with self.Session() as session:
            # Verify the user has access to the filter (and therefore the stream).
            f = session.scalar(
                Filter.select(session.user_or_token, mode="read").where(
                    Filter.id == data["filter_id"]
                )
            )
            if f is None:
                return self.error("Filter not found or access denied", status=403)

            # IDs arrive as strings (run_filter converts _id to str for JS
            # safety); convert back to integers for the MongoDB $match.
            ids = [int(id_) for id_ in data["ids"]]
            collection = data["selectedCollection"]

            headers = {
                "Authorization": f"Bearer {boom_token}",
                "Content-Type": "application/json",
            }

            response = requests.post(
                f"{boom_url}/queries/pipeline",
                json={
                    "catalog_name": collection,
                    "pipeline": [{"$match": {"_id": {"$in": ids}}}],
                    "max_time_ms": 60000,
                },
                headers=headers,
            )

            if response.status_code != 200:
                return self.error(
                    f"Error querying Boom: {response.status_code} {response.text}"
                )

            results = response.json().get("data", [])

            # Preserve the ID order that the frontend supplied so downstream
            # cursor logic stays consistent.
            id_order = {id_: i for i, id_ in enumerate(ids)}
            results.sort(key=lambda doc: id_order.get(doc.get("_id"), 0))

            results = [{**doc, "_id": str(doc["_id"])} for doc in results]
            res = convert_large_ints({"data": {"results": results}})
        return self.success(data=res)

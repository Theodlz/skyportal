# from pymongo import MongoClient
from datetime import datetime, timedelta

import requests
from marshmallow.exceptions import ValidationError
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

from baselayer.app.access import auth_or_token, permissions
from baselayer.app.env import load_env
from baselayer.log import make_log

from ...models import Filter
from ..base import BaseHandler

log = make_log("app/boom-run-filter")

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
        auth_response = requests.post(auth_url, json=boom_credentials)
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
        # we should have a boom_url
        if boom_url is None or boom_credentials is None:
            raise ValueError("Boom is not available")
        # if we don't have a token or it's about to expire (<30min), get another one
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


class BoomRunFilterHandler(BaseHandler):
    @boom_available
    def post(self):
        data = self.get_json()
        with self.Session() as session:
            data_url = f"{boom_url}/queries/pipeline"
            data_payload = {
                "catalog_name": data["selectedCollection"],
                "max_time_ms": 30000,
                "pipeline": data["pipeline"],
            }

            headers = {
                "Authorization": f"Bearer {boom_token}",
                "Content-Type": "application/json",
            }
            response = requests.post(
                data_url, json=data_payload, headers=headers
            )
            response.raise_for_status()
            res = response.json()
        return self.success(data=res)

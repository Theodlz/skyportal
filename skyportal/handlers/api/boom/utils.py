from datetime import datetime, timedelta

import requests

from baselayer.app.env import load_env
from baselayer.log import make_log

log = make_log("app/boom-utils")

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


# JavaScript's Number.MAX_SAFE_INTEGER (2^53 - 1)
MAX_SAFE_INTEGER = 2**53 - 1


def convert_large_ints(obj):
    """Recursively convert integers that exceed JS Number.MAX_SAFE_INTEGER to strings.

    JavaScript cannot represent integers larger than 2^53 - 1 without loss of
    precision. This function walks the response tree and converts any
    out-of-range integer to its string representation so the browser receives
    the exact value.
    """
    if isinstance(obj, bool):
        return obj
    if isinstance(obj, int):
        if obj > MAX_SAFE_INTEGER or obj < -MAX_SAFE_INTEGER:
            return str(obj)
        return obj
    if isinstance(obj, dict):
        return {k: convert_large_ints(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_large_ints(item) for item in obj]
    return obj

import datetime
from baselayer.app.access import permissions
import jwt

from ..base import BaseHandler


class SocketAppHandler(BaseHandler):
    @permissions(["Websocket Apps"])
    def get(self):
        user = self.associated_user_object
        if user is None:
            raise RuntimeError("No current user while authenticating socket. ")

        secret = self.cfg["app.secret_key"]
        token = jwt.encode(
            {
                "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=15),
                "user_id": str(user.id),
            },
            secret,
        )
        return self.success({"token": token})

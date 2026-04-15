import requests

from baselayer.log import make_log

from ....models import Filter
from ...base import BaseHandler
from .utils import boom_available, boom_token, boom_url, convert_large_ints

log = make_log("app/boom-run-filter-full")


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

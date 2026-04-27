import requests

from baselayer.log import make_log

from ...base import BaseHandler
from .utils import boom_available, boom_token, boom_url, convert_large_ints

log = make_log("app/boom-get-alerts")


class BoomAlertsHandler(BaseHandler):
    """Fetch full alert documents by survey and candidate IDs."""

    @boom_available
    def get(self):
        """Retrieve full alert documents for a given survey and list of candidate IDs.

        ---
        description: |
          Given a survey name and a comma-separated list of candidate IDs,
          fetches complete alert documents from Boom via the ``/queries/find``
          endpoint against ``{SURVEY}_alerts``.
        parameters:
          - in: query
            name: survey
            schema:
              type: string
            required: true
            description: Survey name (e.g. ZTF, LSST).
          - in: query
            name: candids
            schema:
              type: string
            required: true
            description: Comma-separated list of integer candidate IDs.
        responses:
          200:
            content:
              application/json:
                schema:
                  allOf:
                    - $ref: '#/components/schemas/Success'
                    - type: object
                      properties:
                        data:
                          type: object
                          properties:
                            results:
                              type: array
                              items:
                                type: object
                              description: Full alert documents, ordered by input ID list.
          400:
            content:
              application/json:
                schema: Error
        """
        survey = self.get_query_argument("survey", None)
        if not survey:
            return self.error("Missing required parameter: survey")

        candids_str = self.get_query_argument("candids", None)
        if not candids_str:
            return self.error("Missing required parameter: candids")

        try:
            candids = [int(c) for c in candids_str.split(",") if c.strip()]
        except ValueError:
            return self.error("candids must be a comma-separated list of integers")

        if not candids:
            return self.error("candids list is empty")

        collection = f"{survey.upper()}_alerts"

        headers = {
            "Authorization": f"Bearer {boom_token}",
            "Content-Type": "application/json",
        }

        response = requests.post(
            f"{boom_url}/queries/find",
            json={
                "catalog_name": collection,
                "filter": {"_id": {"$in": candids}},
                "max_time_ms": 60000,
            },
            headers=headers,
        )

        if response.status_code != 200:
            return self.error(
                f"Error querying Boom: {response.status_code} {response.text}"
            )

        results = response.json().get("data", [])

        # Preserve the ID order supplied by the caller so downstream
        # cursor logic stays consistent.
        id_order = {candid: i for i, candid in enumerate(candids)}
        results.sort(key=lambda doc: id_order.get(doc.get("_id"), 0))

        results = [{**doc, "_id": str(doc["_id"])} for doc in results]
        res = convert_large_ints({"data": {"results": results}})
        return self.success(data=res)

from baselayer.app.access import auth_or_token
import requests
import pandas as pd
import io

from ..base import BaseHandler
from ...models import Instrument
from astropy.time import Time
from .photometry import add_external_photometry

bands = {1: 'ztfg', 2: 'ztfr', 3: 'ztfi'}


class FinkPhotometryHandler(BaseHandler):
    @auth_or_token
    def post(self, object_id):
        print(object_id)
        if not isinstance(object_id, str):
            return self.error("Invalid object ID")
        object_id = object_id.strip()
        r = requests.post(
            'https://fink-portal.org/api/v1/objects',
            json={'objectId': object_id, 'output-format': 'json'},
        )

        # Format output in a DataFrame
        df_request = pd.read_json(io.BytesIO(r.content))
        print(df_request.head())
        for column in df_request.columns:
            print(column)
        desired_columns = [
            'i:objectId',
            'i:ra',
            'i:dec',
            'i:magpsf',
            'i:sigmapsf',
            'i:diffmaglim',
            'i:fid',
            'i:jd',
        ]
        if not set(desired_columns).issubset(set(df_request.columns)):
            return self.error('Missing expected column')

        with self.Session() as session:
            stmt = Instrument.select(session.user_or_token).where(
                Instrument.name == 'CFH12k' or Instrument.name == 'ZTF'
            )
            instrument = session.scalars(stmt).first()
            instrument_id = instrument.id

        print('About to create the data frame')
        data = {
            'object_id': df_request['i:objectId'],
            'ra': df_request['i:ra'],
            'dec': df_request['i:dec'],
            'mag': df_request['i:magpsf'],
            'magerr': df_request['i:sigmapsf'],
            'limiting_mag': df_request['i:diffmaglim'],
            'filter': [bands[band] for band in df_request['i:fid']],
            'mjd': [Time(jd, format="jd").mjd for jd in df_request["i:jd"]],
            'magsys': ['ab' for i in range(len(df_request))],
            'instrument_id': [instrument_id for i in range(len(df_request))],
            'group_ids': [1 for i in range(len(df_request))],
        }

        print('Now converting to dataframe')

        df = pd.DataFrame(data)

        print(df.head(10))

        if len(df.index) > 0:
            print('Adding photometry')
            add_external_photometry(data, self.associated_user_object)
            print('Done adding photometry')

        return self.success()

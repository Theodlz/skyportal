import os
import uuid
import time
from astropy.table import Table

from skyportal.tests import api


def test_galaxy(super_admin_token, view_only_token):

    catalog_name = str(uuid.uuid4())
    datafile = f'{os.path.dirname(__file__)}/../../../data/CLU_mini.hdf5'
    data = {
        'catalog_name': catalog_name,
        'catalog_data': Table.read(datafile).to_pandas().to_dict(orient='list'),
    }

    status, data = api('POST', 'galaxy_catalog', data=data, token=super_admin_token)
    assert status == 200
    assert data['status'] == 'success'

    params = {'catalog_name': catalog_name}

    nretries = 0
    galaxies_loaded = False
    while not galaxies_loaded and nretries < 5:
        try:
            status, data = api(
                'GET', 'galaxy_catalog', token=view_only_token, params=params
            )
            assert status == 200
            data = data["data"]["sources"]
            assert len(data) == 10
            assert any(
                [
                    d['name'] == '6dFgs gJ0001313-055904'
                    and d['mstar'] == 336.60756522868667
                    for d in data
                ]
            )
            galaxies_loaded = True
        except AssertionError:
            nretries = nretries + 1
            time.sleep(5)

    datafile = f'{os.path.dirname(__file__)}/../data/GW220603_preliminary.xml'
    with open(datafile, 'rb') as fid:
        payload = fid.read()
    data = {'xml': payload}

    status, data = api('POST', 'gcn_event', data=data, token=super_admin_token)
    assert status == 200
    assert data['status'] == 'success'

    # wait for tiles to load
    time.sleep(15)

    params = {
        'includeGeoJSON': True,
        'catalog_name': catalog_name,
        'localizationDateobs': '2022-06-03T00:04:12',
        'localizationCumprob': 0.92,
    }

    status, data = api('GET', 'galaxy_catalog', token=view_only_token, params=params)
    assert status == 200

    geojson = data["data"]["geojson"]
    data = data["data"]["sources"]

    # now we have restricted to only 2/10 being in localization
    assert len(data) == 2
    assert any(
        [
            d['name'] == '2MASX J00021772-4345168' and d['mstar'] == 19468772606.159004
            for d in data
        ]
    )

    # The GeoJSON takes the form of
    """
    {"type": "FeatureCollection", "features": [{"geometry": {"coordinates": [0.57383, -43.75467], "type": "Point"}, "properties": {"name": "2MASX J00021772-4345168"}, "type": "Feature"}, {"geometry": {"coordinates": [0.99855, -36.28124], "type": "Point"}, "properties": {"name": "MRSS 349-058718"}, "type": "Feature"}]}
    """

    assert any(
        [
            d['geometry']['coordinates'] == [0.57383, -43.75467]
            and d['properties']['name'] == '2MASX J00021772-4345168'
            for d in geojson['features']
        ]
    )

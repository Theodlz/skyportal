import base64
import gzip
import io
import traceback

import matplotlib.pyplot as plt
import numpy as np
import requests
import sqlalchemy as sa
from astropy.io import fits
from astropy.visualization import (
    AsinhStretch,
    AsymmetricPercentileInterval,
    ImageNormalize,
    LinearStretch,
    LogStretch,
    MinMaxInterval,
    SqrtStretch,
    ZScaleInterval,
)
from scipy.ndimage import rotate
from sqlalchemy.orm.session import Session

from baselayer.app.access import auth_or_token, permissions
from baselayer.app.env import load_env
from baselayer.app.flow import Flow
from baselayer.log import make_log

from ....models import (
    DBSession,
    Group,
    Instrument,
    Obj,
    ObjToSuperObj,
    Source,
    Stream,
    SuperObj,
    Thumbnail,
    User,
)
from ....utils.asynchronous import run_async
from ....utils.parse import str_to_bool
from ...base import BaseHandler
from ..photometry import add_external_photometry
from ..thumbnail import post_thumbnail
from .utils import boom_available, boom_token, boom_url, convert_large_ints

thumbnail_types = [
    ("cutoutScience", "new"),
    ("cutoutTemplate", "ref"),
    ("cutoutDifference", "sub"),
]


def make_thumbnail(
    obj_id, cutout_data, cutout_type: str, thumbnail_type: str, survey: str
):
    rotpa = None
    if isinstance(cutout_data, list):
        # looks like we got it as an array of u8 instead of a bytes object, let's convert it to bytes
        cutout_data = bytes(cutout_data)
    elif isinstance(cutout_data, str):
        # looks like we got it as a base64 string, let's decode it
        cutout_data = base64.b64decode(cutout_data)
    if survey == "LSST":  # LSST uses no compression
        with fits.open(io.BytesIO(cutout_data), ignore_missing_simple=True) as hdu:
            rotpa = hdu[0].header.get("ROTPA", None)
            data = hdu[0].data
    else:
        with (
            gzip.open(io.BytesIO(cutout_data), "rb") as f,
            fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu,
        ):
            rotpa = hdu[0].header.get("ROTPA", None)
            data = hdu[0].data

    buff = io.BytesIO()
    plt.close("all")
    fig = plt.figure()
    fig.set_size_inches(4, 4, forward=False)
    ax = plt.Axes(fig, [0.0, 0.0, 1.0, 1.0])
    ax.set_axis_off()
    fig.add_axes(ax)

    # Clean the data
    img = np.array(data)
    xl = ~np.isnan(img) & (np.abs(img) > 1e20)
    if img[xl].any():
        img[xl] = np.nan
    if np.isnan(img).any():
        median = float(np.nanmean(img.flatten()))
        img = np.nan_to_num(img, nan=median)

    # Normalize
    stretch = LinearStretch() if cutout_type == "cutoutDifference" else LogStretch()
    norm = ImageNormalize(img, stretch=stretch)
    img_norm = norm(img)

    normalizer = AsymmetricPercentileInterval(lower_percentile=1, upper_percentile=100)
    vmin, vmax = normalizer.get_limits(img_norm)

    # Survey-specific transformations to get North up and West on the right
    if survey == "ZTF":
        # flip the image in the vertical direction
        img_norm = np.flipud(img_norm)
    elif survey == "LSST" and rotpa is not None:
        try:
            # Rotate clockwise by ROTPA degrees, reshape to avoid cropping, fill blanks with 0
            img_norm = rotate(
                img_norm,
                -rotpa,
                reshape=True,
                order=1,
                mode="constant",
                cval=0.0,
            )
        except Exception as e:
            # If scipy is not available or rotation fails, skip rotation
            log(f"Failed to rotate LSST image for obj_id {obj_id}: {e}")

    ax.imshow(img_norm, cmap="bone", origin="lower", vmin=vmin, vmax=vmax)

    plt.savefig(buff, format="png", dpi=42)

    buff.seek(0)
    plt.close("all")

    thumbnail_dict = {
        "obj_id": obj_id,
        "data": base64.b64encode(buff.read()).decode("utf-8"),
        "ttype": thumbnail_type,
    }

    return thumbnail_dict


def add_thumbnails(alert, survey, session):
    for cutout_type, thumbnail_type in thumbnail_types:
        if cutout_type not in alert:
            log(f"Cutout key {cutout_type} not found in alert")
            continue
        try:
            thumbnail = make_thumbnail(
                alert["objectId"],
                alert[cutout_type],
                cutout_type,
                thumbnail_type,
                survey,
            )
        except Exception as e:
            traceback.print_exc()
            log(f"Failed to create thumbnail for cutout type {cutout_type}: {e}")
            continue
        post_thumbnail(thumbnail, user_id=1, session=session)


def fetch_and_add_thumbnails(obj_id, survey, headers, obj_internal_key=None):
    with DBSession() as session:
        try:
            existing_thumbnails = session.scalars(
                sa.select(Thumbnail).where(Thumbnail.obj_id == obj_id)
            ).all()
            existing_thumbnail_types = {t.type for t in existing_thumbnails}
            if all(t in existing_thumbnail_types for t in ["new", "ref", "sub"]):
                return
            cutouts_response = requests.get(
                f"{boom_url}/surveys/{survey.upper()}/cutouts",
                headers=headers,
                params={"objectId": obj_id, "which": "brightest"},
                timeout=30,
            )
            if cutouts_response.status_code != 200:
                log(
                    f"Error querying Boom API for cutouts: {cutouts_response.status_code} {cutouts_response.text}"
                )
                return
            cutout_data = cutouts_response.json().get("data", {})
            if not cutout_data:
                log(f"No cutout data found for object {obj_id} in survey {survey}")
                return
            cutout_data["objectId"] = obj_id
            add_thumbnails(cutout_data, survey, session)
            session.commit()
        except Exception as e:
            log(f"Failed to fetch or add thumbnails for obj_id {obj_id}: {e}")
            traceback.print_exc()

    if obj_internal_key is not None:
        try:
            flow = Flow()
            flow.push(
                "*",
                "skyportal/REFRESH_SOURCE",
                payload={"obj_key": obj_internal_key},
            )
        except Exception as e:
            log(f"Failed to send notification: {e}")


ZP_PER_SURVEY = {"LSST": 8.9, "ZTF": 23.9}

log = make_log("api/boom_alerts")

_, cfg = load_env()


def make_programid2stream_mapper(session: Session):
    # here we:
    # - get all the streams
    # - each stream has an altdata field that looks like: "`{'collection': 'ZTF_alerts', selector: [1, 2]}`"
    # - using the altdata's content we create a mapper where given a survey name and a programid we get the streams
    # - basically each stream with a given survey name and programid in its selector is associated with a programid
    streams = session.scalars(sa.select(Stream)).all()
    mapper = {}
    for stream in streams:
        altdata = stream.altdata
        if altdata is None or "collection" not in altdata or "selector" not in altdata:
            continue
        survey = altdata["collection"].split("_")[0]
        programid = max(altdata["selector"])
        key = (survey, programid)
        if key not in mapper:
            mapper[key] = set()
        mapper[(survey, programid)].add(stream.id)

    # convert from set to list
    for key in mapper:
        mapper[key] = list(mapper[key])
    return mapper


def make_survey2instrumentid(session: Session):
    ztf_instrument_id = session.scalar(
        sa.select(Instrument.id).where(Instrument.name == "ZTF")
    )
    if ztf_instrument_id is None:
        raise ValueError("Instrument ZTF not found in the database")
    lsst_instrument_id = session.scalar(
        sa.select(Instrument.id).where(Instrument.name == "LSST")
    )
    if lsst_instrument_id is None:
        raise ValueError("Instrument LSST not found in the database")
    survey2instrumentid = {"ZTF": ztf_instrument_id, "LSST": lsst_instrument_id}
    return survey2instrumentid


def process_photometry(
    object_id, survey, data, survey2instrumentid, programid2streamid, user, session
):
    instrument_id = survey2instrumentid.get(survey)
    if instrument_id is None:
        log(f"No instrument found for survey {survey}, skipping photometry")
        raise ValueError(f"No instrument found for survey {survey}")
    zp = ZP_PER_SURVEY.get(survey)
    if zp is None:
        log(f"No zero point found for survey {survey}, skipping photometry")
        raise ValueError(f"No zero point found for survey {survey}")

    photometry_data = {}
    for array_name in ["prv_candidates", "prv_nondetections", "fp_hists"]:
        phot_array = data.get(array_name)
        if phot_array is None:
            continue
        for phot in phot_array:
            programid = phot["programid"] if survey == "ZTF" else 1
            key = (survey, programid)
            if key not in photometry_data:
                stream_ids = programid2streamid.get(key)
                if stream_ids is None:
                    log(
                        f"No stream found for survey {survey} and programid {programid}, skipping photometry"
                    )
                    continue
                photometry_data[key] = {
                    "obj_id": object_id,
                    "stream_ids": stream_ids,
                    "instrument_id": instrument_id,
                    "mjd": [],
                    "flux": [],
                    "fluxerr": [],
                    "filter": [],
                    "zp": [],
                    "magsys": [],
                    "ra": [],
                    "dec": [],
                }

            photometry_data[key]["mjd"].append(phot["jd"] - 2400000.5)
            flux = phot.get("psfFlux", None)
            flux_err = phot.get("psfFluxErr", None) * 1e-9
            if flux is not None and not np.isnan(flux):
                flux = flux * 1e-9
                # if abs(flux) / flux_err <= 3, we set flux to NaN (non detection)
                if (
                    flux_err is not None
                    and not np.isnan(flux_err)
                    and abs(flux) / flux_err <= 3
                ):
                    flux = np.nan
            photometry_data[key]["flux"].append(flux)
            photometry_data[key]["fluxerr"].append(flux_err)
            photometry_data[key]["filter"].append(
                f"{str(survey).lower()}{str(phot['band']).lower()}"
            )
            photometry_data[key]["zp"].append(zp)
            photometry_data[key]["magsys"].append("ab")
            photometry_data[key]["ra"].append(phot.get("ra"))
            photometry_data[key]["dec"].append(phot.get("dec"))

    for key, data in photometry_data.items():
        add_external_photometry(data, user, session)


BOOM_RADIUS_UNIT_MAP = {
    "deg": "Degrees",
    "arcmin": "Arcminutes",
    "arcsec": "Arcseconds",
}

NO_CUTOUT_PROJECTION = {
    "cutoutScience": 0,
    "cutoutTemplate": 0,
    "cutoutDifference": 0,
}


class BoomAlertHandler(BaseHandler):
    @auth_or_token
    @boom_available
    async def get(self, survey: str):
        """
        ---
        summary: Retrieve alerts from Boom for a given survey
        description: |
          Retrieve alerts from Boom by objectId (single or comma-separated list),
          candid, or sky position (ra/dec/radius/radius_units). Positional queries
          and objectId filtering can be combined.
        tags:
          - alerts
          - boom
        parameters:
          - in: path
            name: survey
            required: true
            schema:
              type: string
            description: Survey name (e.g. ZTF, LSST)
          - in: query
            name: objectId
            required: false
            schema:
              type: string
            description: Single objectId or comma-separated list of objectIds
          - in: query
            name: candid
            required: false
            schema:
              type: integer
            description: Alert candid. Can be combined with objectId.
          - in: query
            name: ra
            required: false
            schema:
              type: number
            description: RA in degrees
          - in: query
            name: dec
            required: false
            schema:
              type: number
            description: Declination in degrees
          - in: query
            name: radius
            required: false
            schema:
              type: number
            description: Search radius (capped at 1 deg). Units set by radius_units.
          - in: query
            name: radius_units
            required: false
            schema:
              type: string
              enum: [deg, arcmin, arcsec]
            description: Units for radius
        responses:
          200:
            description: retrieved alert(s)
            content:
              application/json:
                schema:
                  allOf:
                    - $ref: '#/components/schemas/Success'
                    - type: object
                      properties:
                        data:
                          type: array
                          items:
                            type: object
          400:
            content:
              application/json:
                schema: Error
        """
        object_id = self.get_query_argument("objectId", None)
        candid = self.get_query_argument("candid", None)
        ra = self.get_query_argument("ra", None)
        dec = self.get_query_argument("dec", None)
        radius = self.get_query_argument("radius", None)
        radius_units = self.get_query_argument("radius_units", None)

        position_tuple = (ra, dec, radius, radius_units)

        if not any((object_id, candid, ra, dec, radius, radius_units)):
            return self.error(
                "Missing required parameters: provide objectId, candid, or ra/dec/radius/radius_units."
            )

        headers = {"Authorization": f"Bearer {boom_token}"}
        catalog = f"{survey.upper()}_alerts"

        try:
            if candid is not None:
                try:
                    candid = int(candid)
                except ValueError:
                    return self.error("`candid` must be an integer.")

                filter_doc = {"candid": candid}
                if object_id:
                    filter_doc["objectId"] = object_id

                response = requests.post(
                    f"{boom_url}/queries/find",
                    headers=headers,
                    json={
                        "catalog_name": catalog,
                        "filter": filter_doc,
                        "projection": NO_CUTOUT_PROJECTION,
                        "max_time_ms": 10000,
                    },
                    timeout=15,
                )
                if response.status_code != 200:
                    return self.error(
                        f"Boom query failed: {response.status_code} {response.text}"
                    )
                data = response.json().get("data", [])
                return self.success(data=convert_large_ints(data))

            if not any(position_tuple):
                # objectId-only query
                if object_id is None:
                    return self.error("Missing required parameters.")

                object_ids = [oid.strip() for oid in object_id.split(",")]
                filter_doc = (
                    {"objectId": object_ids[0]}
                    if len(object_ids) == 1
                    else {"objectId": {"$in": object_ids}}
                )

                response = requests.post(
                    f"{boom_url}/queries/find",
                    headers=headers,
                    json={
                        "catalog_name": catalog,
                        "filter": filter_doc,
                        "projection": NO_CUTOUT_PROJECTION,
                        "max_time_ms": 10000,
                    },
                    timeout=15,
                )
                if response.status_code != 200:
                    return self.error(
                        f"Boom query failed: {response.status_code} {response.text}"
                    )
                data = response.json().get("data", [])
                return self.success(data=convert_large_ints(data))

            # Positional query
            if not all(position_tuple):
                missing = [
                    name
                    for name, val in zip(
                        ["ra", "dec", "radius", "radius_units"], position_tuple
                    )
                    if val is None
                ]
                return self.error(f"Missing positional parameters: {missing}.")

            if radius_units not in BOOM_RADIUS_UNIT_MAP:
                return self.error(
                    "Invalid radius_units. Must be one of 'deg', 'arcmin', or 'arcsec'."
                )
            try:
                ra = float(ra)
                dec = float(dec)
                radius = float(radius)
            except ValueError:
                return self.error("Invalid (non-float) value provided.")

            if (
                (radius_units == "deg" and radius > 1)
                or (radius_units == "arcmin" and radius > 60)
                or (radius_units == "arcsec" and radius > 3600)
            ):
                return self.error("Radius must be <= 1.0 deg.")

            response = requests.post(
                f"{boom_url}/queries/cone_search",
                headers=headers,
                json={
                    "catalog_name": catalog,
                    "object_coordinates": {"query": [ra, dec]},
                    "radius": radius,
                    "unit": BOOM_RADIUS_UNIT_MAP[radius_units],
                    "max_time_ms": 10000,
                },
                timeout=15,
            )
            if response.status_code != 200:
                return self.error(
                    f"Boom cone search failed: {response.status_code} {response.text}"
                )

            alert_data = response.json().get("data", {}).get("query", [])

            if object_id is not None:
                filter_ids = {oid.strip() for oid in object_id.split(",")}
                alert_data = [a for a in alert_data if a.get("objectId") in filter_ids]

            return self.success(data=convert_large_ints(alert_data))

        except Exception:
            _err = traceback.format_exc()
            return self.error(f"failure: {_err}")


class BoomObjectHandler(BaseHandler):
    @permissions(["Upload data"])
    @boom_available
    def post(self, survey, object_id):
        """
        ---
        summary: Import an alert from Boom for a given survey and object ID
        description: Import an alert from Boom for a given survey and object ID
        tags:
            - alerts
            - boom
        """
        data = self.get_json()
        group_ids = data.pop("group_ids", None)
        try:
            group_ids = [int(gid) for gid in group_ids]
        except Exception:
            return self.error(
                "Invalid `group_ids` parameter. Must be a list of integers."
            )

        with self.Session() as session:
            if not self.associated_user_object.is_admin:
                accessible_groups = [
                    g.id for g in self.associated_user_object.accessible_groups
                ]
                if not all(gid in accessible_groups for gid in group_ids):
                    return self.error(
                        "You do not have access to all the groups provided in `group_ids`."
                    )

            # validate that all the groups exist in the database
            groups = session.scalars(
                sa.select(Group).where(Group.id.in_(group_ids))
            ).all()
            if len(groups) != len(group_ids):
                existing_group_ids = {g.id for g in groups}
                missing_group_ids = [
                    gid for gid in group_ids if gid not in existing_group_ids
                ]
                return self.error(
                    f"The following group IDs do not exist (or are not accessible): {missing_group_ids}"
                )

            user = session.scalar(sa.select(User).where(User.id == 1))
            if user is None:
                log("User with id 1 not found in the database")
                return self.error("Internal error: admin user not found")
            survey2instrumentid = make_survey2instrumentid(session)
            programid2streamid = make_programid2stream_mapper(session)

            # Retrieve the data
            url = f"{boom_url}/queries/pipeline"
            headers = {"Authorization": f"Bearer {boom_token}"}
            json_data = {
                "catalog_name": f"{str(survey).upper()}_alerts",
                "pipeline": [
                    {"$match": {"objectId": object_id}},
                    # sort by candidate.magpsf ascending (brightest first)
                    {"$sort": {"candidate.magpsf": 1}},
                    # only keep one candidate per object (the brightest one)
                    {"$group": {"_id": "$objectId", "data": {"$first": "$$ROOT"}}},
                    # replace the root with the data field
                    {"$replaceRoot": {"newRoot": "$data"}},
                    # now do a lookup to the {SURVEY}_alerts_aux collection to get the rest of the data
                    {
                        "$lookup": {
                            "from": f"{str(survey).upper()}_alerts_aux",
                            "localField": "objectId",
                            "foreignField": "_id",
                            "as": "aux",
                        }
                    },
                    # unwind the aux array
                    {"$unwind": {"path": "$aux", "preserveNullAndEmptyArrays": True}},
                    # final projection to remove the aux field
                    {
                        "$project": {
                            "_id": 1,
                            "objectId": 1,
                            "candidate": 1,
                            "prv_candidates": "$aux.prv_candidates",
                            "prv_nondetections": "$aux.prv_nondetections",
                            "fp_hists": "$aux.fp_hists",
                        }
                    },
                ],
                "max_time_ms": 30000,  # 30 second timeout
            }
            response = requests.post(url, headers=headers, json=json_data)
            if response.status_code != 200:
                log(f"Error querying Boom API: {response.status_code} {response.text}")
                return self.error(
                    f"Error querying Boom API: {response.status_code} {response.text}"
                )
            if "data" not in response.json() or len(response.json()["data"]) == 0:
                log(f"No data found for object {object_id} in survey {survey}")
                return self.error(
                    f"No data found for object {object_id} in survey {survey}"
                )

            data = response.json()["data"][0]

            obj = session.scalar(sa.select(Obj).where(Obj.id == data["objectId"]))
            if not obj:
                # create the obj and save it to the groups
                obj = Obj(
                    id=data["objectId"],
                    ra=data["candidate"]["ra"],
                    dec=data["candidate"]["dec"],
                    ra_dis=data["candidate"]["ra"],
                    dec_dis=data["candidate"]["dec"],
                    score=data["candidate"].get("drb"),
                    origin=f"BOOM",
                )
                session.add(obj)
                for g in groups:
                    session.add(
                        Source(
                            obj=obj, group=g, saved_by_id=self.associated_user_object.id
                        )
                    )
            else:
                # if the obj already exists, we just save it to new groups if any
                existing_sources = session.scalars(
                    sa.select(Source).where(
                        Source.obj_id == obj.id, Source.group_id.in_(group_ids)
                    )
                ).all()
                existing_group_ids = {s.group_id for s in existing_sources}
                new_groups = [g for g in groups if g.id not in existing_group_ids]
                for g in new_groups:
                    session.add(
                        Source(
                            obj=obj, group=g, saved_by_id=self.associated_user_object.id
                        )
                    )

            process_photometry(
                object_id,
                survey,
                data,
                survey2instrumentid,
                programid2streamid,
                user,
                session,
            )

            # at the coordinates of obj, query the other survey's alerts to see if there's a match within 1 arcsec, if so add photometry from that survey as well
            other_obj = None
            other_survey = "LSST" if survey == "ZTF" else "ZTF"
            other_url = f"{boom_url}/queries/cone_search"
            other_json_data = {
                "catalog_name": f"{str(other_survey).upper()}_alerts_aux",
                "object_coordinates": {
                    object_id: [data["candidate"]["ra"], data["candidate"]["dec"]]
                },
                "radius": 2,  # 2 arcsec
                "unit": "Arcseconds",
                "max_time_ms": 30000,  # 30 second timeout
            }
            other_response = requests.post(
                other_url, headers=headers, json=other_json_data
            )
            if other_response.status_code != 200:
                log(
                    f"Error querying Boom API for matching surveys' photometry: {other_response.status_code} {other_response.text}"
                )
                return self.error(
                    f"Error querying Boom API for matching surveys' photometry: {other_response.status_code} {other_response.text}"
                )
            else:
                other_data = other_response.json().get("data", {})
                if object_id in other_data and len(other_data[object_id]) > 0:
                    other_alert = other_data[object_id][0]  # take the closest match
                    existing_obj = session.scalar(
                        sa.select(Obj).where(Obj.id == other_alert["_id"])
                    )
                    if not existing_obj:
                        other_obj = Obj(
                            id=other_alert["_id"],
                            ra=other_alert["coordinates"]["radec_geojson"][
                                "coordinates"
                            ][0]
                            + 180,  # go from GeoJSON (-180 to 180) to RA (0 to 360)
                            dec=other_alert["coordinates"]["radec_geojson"][
                                "coordinates"
                            ][1],
                            ra_dis=other_alert["coordinates"]["radec_geojson"][
                                "coordinates"
                            ][0]
                            + 180,
                            dec_dis=other_alert["coordinates"]["radec_geojson"][
                                "coordinates"
                            ][1],
                            origin=f"BOOM",
                        )
                        session.add(other_obj)
                    process_photometry(
                        other_alert["_id"],
                        other_survey,
                        other_alert,
                        survey2instrumentid,
                        programid2streamid,
                        user,
                        session,
                    )

                    # if there isn't one already, we create a SuperObj and associate both the ZTF and LSST Obj with it, so that in the future we can easily query all associated objects across surveys
                    existing_associations = session.scalars(
                        sa.select(ObjToSuperObj).where(
                            ObjToSuperObj.obj_id.in_(
                                [data["objectId"], other_alert["_id"]]
                            )
                        )
                    ).all()
                    if len(existing_associations) == 0:
                        superobj = SuperObj()
                        session.add(superobj)
                        session.flush()  # to get the superobj.id
                        association1 = ObjToSuperObj(
                            obj_id=data["objectId"], super_obj_id=superobj.id
                        )
                        association2 = ObjToSuperObj(
                            obj_id=other_alert["_id"], super_obj_id=superobj.id
                        )
                        session.add_all([association1, association2])

            session.commit()

            obj_internal_key = obj.internal_key
            other_obj_id, other_obj_internal_key = None, None
            if other_obj is not None:
                other_obj_id = other_obj.id
                other_obj_internal_key = other_obj.internal_key

            run_async(
                fetch_and_add_thumbnails, object_id, survey, headers, obj_internal_key
            )
            if other_obj_id is not None:
                run_async(
                    fetch_and_add_thumbnails,
                    other_obj_id,
                    other_survey,
                    headers,
                    other_obj_internal_key,
                )

        return self.success({"survey": survey, "objectId": object_id})


class BoomAlertAuxHandler(BaseHandler):
    @auth_or_token
    @boom_available
    async def get(self, survey: str, object_id: str):
        """
        ---
        summary: Retrieve aux data for an objectId from Boom
        description: |
          Retrieves auxiliary data for a given objectId from Boom, including
          previous candidates, forced-photometry history, non-detections, and
          cross-matches. Also appends the most recent alert detection(s) from
          the alerts collection when they are absent from prv_candidates.
        tags:
          - alerts
          - boom
        parameters:
          - in: path
            name: survey
            required: true
            schema:
              type: string
            description: Survey name (e.g. ZTF, LSST)
          - in: path
            name: object_id
            required: true
            schema:
              type: string
          - in: query
            name: includePrvCandidates
            required: false
            schema:
              type: boolean
            default: true
          - in: query
            name: includeFpHists
            required: false
            schema:
              type: boolean
            default: true
          - in: query
            name: includePrvNondetections
            required: false
            schema:
              type: boolean
            default: true
          - in: query
            name: includeAllFields
            required: false
            schema:
              type: boolean
            default: false
        responses:
          200:
            description: retrieved aux data
            content:
              application/json:
                schema:
                  allOf:
                    - $ref: '#/components/schemas/Success'
                    - type: object
                      properties:
                        data:
                          type: object
          400:
            content:
              application/json:
                schema: Error
        """
        include_prv_candidates = str_to_bool(
            self.get_query_argument("includePrvCandidates", "true"), default=True
        )
        include_fp_hists = str_to_bool(
            self.get_query_argument("includeFpHists", "true"), default=True
        )
        include_prv_nondetections = str_to_bool(
            self.get_query_argument("includePrvNondetections", "true"), default=True
        )
        include_all_fields = str_to_bool(
            self.get_query_argument("includeAllFields", "false").lower(), default=False
        )

        headers = {"Authorization": f"Bearer {boom_token}"}
        catalog_aux = f"{survey.upper()}_alerts_aux"

        try:
            # ── 1. Fetch aux document ────────────────────────────────────────
            aux_pipeline = [{"$match": {"_id": object_id}}]
            if not include_all_fields:
                aux_pipeline.append(
                    {
                        "$project": {
                            "_id": 1,
                            "cross_matches": 1,
                            "prv_candidates.candid": 1,
                            "prv_candidates.jd": 1,
                            "prv_candidates.band": 1,
                            "prv_candidates.programid": 1,
                            "prv_candidates.ra": 1,
                            "prv_candidates.dec": 1,
                            "prv_candidates.magpsf": 1,
                            "prv_candidates.sigmapsf": 1,
                            "prv_candidates.diffmaglim": 1,
                            "prv_candidates.isdiffpos": 1,
                            "prv_candidates.snr_psf": 1,
                            "fp_hists.jd": 1,
                            "fp_hists.band": 1,
                            "fp_hists.programid": 1,
                            "fp_hists.ra": 1,
                            "fp_hists.dec": 1,
                            "fp_hists.magpsf": 1,
                            "fp_hists.sigmapsf": 1,
                            "fp_hists.diffmaglim": 1,
                            "fp_hists.isdiffpos": 1,
                            "fp_hists.snr_psf": 1,
                            "prv_nondetections.jd": 1,
                            "prv_nondetections.band": 1,
                            "prv_nondetections.programid": 1,
                            "prv_nondetections.diffmaglim": 1,
                        }
                    }
                )

            aux_response = requests.post(
                f"{boom_url}/queries/pipeline",
                headers=headers,
                json={
                    "catalog_name": catalog_aux,
                    "pipeline": aux_pipeline,
                    "max_time_ms": 10000,
                },
                timeout=15,
            )
            if aux_response.status_code != 200:
                return self.error(
                    f"Boom aux query failed: {aux_response.status_code} {aux_response.text}"
                )

            aux_records = aux_response.json().get("data", [])
            if len(aux_records) > 0:
                aux_data = aux_records[0]
            else:
                aux_data = {
                    "prv_candidates": [],
                    "fp_hists": [],
                    "prv_nondetections": [],
                    "cross_matches": {},
                    "missing": True,
                    "message": (
                        "Aux data for this object is missing from Boom. "
                        "Use alert data directly to retrieve detections."
                    ),
                }

            # ── 2. Compute median coordinates ────────────────────────────────
            all_ras = [
                c["ra"]
                for c in aux_data.get("prv_candidates", [])
                if c.get("ra") is not None
            ]
            all_decs = [
                c["dec"]
                for c in aux_data.get("prv_candidates", [])
                if c.get("dec") is not None
            ]
            if all_ras and all_decs:
                aux_data["coordinates"] = {
                    "ra_median": float(
                        np.median(np.unique(np.array(all_ras).round(decimals=10)))
                    ),
                    "dec_median": float(
                        np.median(np.unique(np.array(all_decs).round(decimals=10)))
                    ),
                }

            # ── 4. Suppress unwanted sections ───────────────────────────────
            if not include_prv_candidates:
                aux_data.pop("prv_candidates", None)
            if not include_fp_hists:
                aux_data.pop("fp_hists", None)
            if not include_prv_nondetections:
                aux_data.pop("prv_nondetections", None)

            return self.success(data=aux_data)

        except Exception:
            _err = traceback.format_exc()
            return self.error(f"failure: {_err}")


class BoomAlertCutoutHandler(BaseHandler):
    @auth_or_token
    @boom_available
    async def get(self, survey: str):
        """
        ---
        summary: Serve Boom alert cutout(s) as JSON (FITS) or PNG
        description: |
          When file_format=fits (default): fetches all three cutouts from Boom
          and returns the raw payload as JSON (keys: cutoutScience,
          cutoutTemplate, cutoutDifference). No server-side processing is
          applied; the caller receives exactly what Boom returned.

          When file_format=png: renders a single cutout type as a PNG image.
          The `cutout` parameter is required in this mode.
        tags:
          - alerts
          - boom

        parameters:
          - in: path
            name: survey
            description: "Survey name (e.g. ZTF, LSST)"
            required: true
            schema:
              type: string
          - in: query
            name: candid
            description: "Alert candid. Mutually exclusive with objectId."
            required: false
            schema:
              type: integer
          - in: query
            name: objectId
            description: "Object ID. Mutually exclusive with candid."
            required: false
            schema:
              type: string
          - in: query
            name: which
            description: "Which alert to use when querying by objectId."
            required: false
            schema:
              type: string
              enum: [first, last, brightest, faintest]
          - in: query
            name: file_format
            description: |
              fits (default): return raw Boom JSON with all three cutouts.
              png: render a single cutout as a PNG image (requires `cutout`).
            required: false
            default: png
            schema:
              type: string
              enum: [fits, png]
          - in: query
            name: cutout
            description: "PNG mode only: which cutout to render."
            required: false
            schema:
              type: string
              enum: [science, template, difference]
          - in: query
            name: interval
            description: "PNG mode only: normalisation interval."
            required: false
            schema:
              type: string
              enum: [min_max, zscale]
          - in: query
            name: stretch
            description: "PNG mode only: stretch function."
            required: false
            schema:
              type: string
              enum: [linear, log, asinh, sqrt]
          - in: query
            name: cmap
            description: "PNG mode only: colour map."
            required: false
            schema:
              type: string
              enum: [bone, gray, cividis, viridis, magma]

        responses:
          '200':
            description: retrieved cutout(s)
            content:
              application/json:
                schema:
                  allOf:
                    - $ref: '#/components/schemas/Success'
                    - type: object
                      properties:
                        data:
                          type: object
              image/png:
                schema:
                  type: string
                  format: binary
          '400':
            description: retrieval failed
            content:
              application/json:
                schema: Error
        """
        import time

        try:
            candid = self.get_query_argument("candid", None)
            object_id = self.get_query_argument("objectId", None)
            which = self.get_query_argument("which", "last")
            file_format = self.get_argument("file_format", "png").lower()
            cutout = self.get_argument("cutout", None)
            interval = self.get_argument("interval", default=None)
            stretch = self.get_argument("stretch", default=None)
            cmap = self.get_argument("cmap", default=None)

            # ── common validation ────────────────────────────────────────────
            if candid is None and object_id is None:
                return self.error("Either `candid` or `objectId` must be provided.")
            if candid is not None and object_id is not None:
                return self.error(
                    "Only one of `candid` or `objectId` should be provided."
                )
            if candid is not None:
                try:
                    candid = int(candid)
                except ValueError:
                    return self.error("`candid` must be an integer.")

            known_file_formats = ["fits", "png"]
            if file_format not in known_file_formats:
                return self.error(f"`file_format` must be one of {known_file_formats}.")

            known_which = ["first", "last", "brightest", "faintest"]
            if which not in known_which:
                return self.error(f"`which` must be one of {known_which}.")

            params = {}
            if candid is not None:
                params["candid"] = candid
            else:
                params["objectId"] = object_id
                params["which"] = which

            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {boom_token}",
            }

            # ── fetch from Boom ──────────────────────────────────────────────
            response = requests.get(
                f"{boom_url}/surveys/{survey.upper()}/cutouts",
                headers=headers,
                params=params,
                timeout=10,
            )

            if response.status_code != 200:
                return self.error(
                    f"Failed to fetch cutout from Boom: {response.status_code} {response.text}"
                )

            resp_json = response.json()
            if "data" not in resp_json:
                return self.error(
                    "Unexpected response from Boom API (missing 'data' field)."
                )

            boom_data = resp_json["data"]

            # ── FITS mode: return raw Boom payload unchanged ─────────────────
            if file_format == "fits":
                return self.success(data=boom_data)

            # ── PNG mode: render one cutout type ─────────────────────────────
            if cutout is None:
                return self.error("`cutout` is required when file_format=png.")
            cutout = cutout.capitalize()
            known_cutouts = ["Science", "Template", "Difference"]
            if cutout not in known_cutouts:
                return self.error(f"`cutout` must be one of {known_cutouts}.")

            cutout_key = f"cutout{cutout}"
            if cutout_key not in boom_data:
                return self.error(f"Cutout type '{cutout}' not found in Boom response.")

            raw_cutout = boom_data[cutout_key]
            if isinstance(raw_cutout, list):
                raw_cutout = bytes(raw_cutout)
            elif isinstance(raw_cutout, str):
                raw_cutout = base64.b64decode(raw_cutout)

            if survey.upper() == "LSST":
                with fits.open(
                    io.BytesIO(raw_cutout), ignore_missing_simple=True
                ) as hdu:
                    header = hdu[0].header
                    data_array = hdu[0].data
            else:
                with gzip.open(io.BytesIO(raw_cutout), "rb") as f:
                    with fits.open(
                        io.BytesIO(f.read()), ignore_missing_simple=True
                    ) as hdu:
                        header = hdu[0].header
                        data_array = hdu[0].data

            if survey.upper() == "ZTF":
                data_array = np.flipud(data_array)
            elif survey.upper() == "LSST":
                rotpa = header.get("ROTPA", None)
                if rotpa is not None:
                    try:
                        data_array = rotate(
                            data_array,
                            -rotpa,
                            reshape=True,
                            order=1,
                            mode="constant",
                            cval=0.0,
                        )
                    except Exception as e:
                        log(f"Failed to rotate LSST image: {e}")

            normalization_methods = {
                "asymmetric_percentile": AsymmetricPercentileInterval(
                    lower_percentile=1, upper_percentile=100
                ),
                "min_max": MinMaxInterval(),
                "zscale": ZScaleInterval(n_samples=600, contrast=0.045, krej=2.5),
            }
            if interval is None:
                interval = "asymmetric_percentile"
            normalizer = normalization_methods.get(
                interval.lower(),
                AsymmetricPercentileInterval(lower_percentile=1, upper_percentile=100),
            )

            stretching_methods = {
                "linear": LinearStretch,
                "log": LogStretch,
                "asinh": AsinhStretch,
                "sqrt": SqrtStretch,
            }
            if stretch is None:
                stretch = "log" if cutout != "Difference" else "linear"
            stretcher = stretching_methods.get(stretch.lower(), LogStretch)()

            if cmap is None or cmap.lower() not in [
                "bone",
                "gray",
                "cividis",
                "viridis",
                "magma",
            ]:
                cmap = "bone"
            else:
                cmap = cmap.lower()

            img = np.array(data_array)
            xl = np.greater(np.abs(img), 1e20, where=~np.isnan(img))
            if img[xl].any():
                img[xl] = np.nan
            if np.isnan(img).any():
                img = np.nan_to_num(img, nan=float(np.nanmean(img.flatten())))

            norm = ImageNormalize(img, stretch=stretcher)
            img_norm = norm(img)
            vmin, vmax = normalizer.get_limits(img_norm)

            buff = io.BytesIO()
            fig, ax = plt.subplots(figsize=(4, 4))
            fig.subplots_adjust(0, 0, 1, 1)
            ax.set_axis_off()
            ax.imshow(img_norm, cmap=cmap, origin="lower", vmin=vmin, vmax=vmax)
            plt.savefig(buff, dpi=42, format="png")
            plt.close(fig)
            buff.seek(0)
            self.set_header("Content-Type", "image/png")
            self.write(buff.getvalue())

        except Exception:
            _err = traceback.format_exc()
            return self.error(f"failure: {_err}")

    @permissions(["Upload data"])
    @boom_available
    def post(self, survey: str):
        """
        ---
        summary: Save or replace cutout thumbnails for an existing source
        description: |
          Fetches cutout images from Boom for a given alert (identified by
          candid or objectId + which) and stores them as thumbnails for the
          corresponding source in SkyPortal. All existing thumbnails of types
          new/ref/sub for that source are replaced. Returns an error if the
          object does not already exist as a source.
        tags:
          - alerts
          - boom
        parameters:
          - in: path
            name: survey
            required: true
            schema:
              type: string
            description: Survey name (e.g. ZTF, LSST)
        requestBody:
          content:
            application/json:
              schema:
                type: object
                required:
                  - objectId
                properties:
                  objectId:
                    type: string
                    description: Object ID of the existing source
                  candid:
                    type: integer
                    description: >
                      Alert candid to use for the cutout. Mutually exclusive
                      with `which`.
                  which:
                    type: string
                    enum: [first, last, brightest, faintest]
                    default: last
                    description: >
                      When querying by objectId, which alert to use.
                      Ignored when `candid` is provided.
                  band:
                    type: string
                    description: Optional band filter (e.g. g, r, i for LSST)
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
          400:
            content:
              application/json:
                schema: Error
        """
        data = self.get_json()
        object_id = data.get("objectId")
        candid = data.get("candid")
        which = data.get("which", "last")
        band = data.get("band")

        if not object_id:
            return self.error("`objectId` is required.")

        known_which = ["first", "last", "brightest", "faintest"]
        if which not in known_which:
            return self.error(f"`which` must be one of {known_which}.")

        if candid is not None:
            try:
                candid = int(candid)
            except (TypeError, ValueError):
                return self.error("`candid` must be an integer.")

        params = {}
        if candid is not None:
            params["candid"] = candid
        else:
            params["objectId"] = object_id
            params["which"] = which
        if band is not None:
            params["band"] = band

        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {boom_token}",
        }

        with self.Session() as session:
            obj = session.scalar(sa.select(Obj).where(Obj.id == object_id))
            if obj is None:
                return self.error(
                    f"Object '{object_id}' not found. Save it as a source first."
                )

            response = requests.get(
                f"{boom_url}/surveys/{survey.upper()}/cutouts",
                headers=headers,
                params=params,
                timeout=10,
            )
            if response.status_code != 200:
                return self.error(
                    f"Failed to fetch cutouts from Boom: "
                    f"{response.status_code} {response.text}"
                )

            resp_json = response.json()
            cutout_data = resp_json.get("data", {})
            cutout_data["objectId"] = object_id

            # Replace any existing stored thumbnails of these types
            existing = session.scalars(
                sa.select(Thumbnail).where(
                    Thumbnail.obj_id == object_id,
                    Thumbnail.type.in_([t[1] for t in thumbnail_types]),
                )
            ).all()
            for thumb in existing:
                session.delete(thumb)
            session.flush()

            add_thumbnails(cutout_data, survey.upper(), session)
            session.commit()

        return self.success(data={"objectId": object_id, "survey": survey})

import datetime
import io
import math
import os
import re
import string
import urllib
import warnings
from functools import wraps

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import seaborn as sns
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.table import Table
from astropy.time import Time
from astropy.utils.exceptions import AstropyWarning
from astropy.visualization import ImageNormalize, ZScaleInterval
from astropy.wcs import WCS
from astropy.wcs.utils import pixel_to_skycoord
from astropy.wcs.wcs import FITSFixedWarning
from joblib import Memory
from reproject import reproject_adaptive
from scipy.ndimage import gaussian_filter

from baselayer.app.env import load_env
from baselayer.log import make_log

from .cache import Cache
from .tap_services.gaia import GaiaQuery

log = make_log("finder-chart")

_, cfg = load_env()

PS1_CUTOUT_TIMEOUT = 15  # seconds

HOST = f"{cfg['server.protocol']}://{cfg['server.host']}" + (
    f":{cfg['server.port']}" if cfg["server.port"] not in [80, 443] else ""
)

NGPS_TARGET_BANDS_TO_SNCOSMO = {
    "G": ["ztfg", "sdssg", "lsstg"],
    "R": ["ztfr", "sdssr", "bessellr", "standard::r", "lsstr"],
    "I": ["ztfi", "sdssi", "besselli", "standard::i", "lssti"],
    "U": ["sdssu", "bessellux", "standard::u", "lsstu"],
}

# we inverse the dictionnary
SNCOSMO_BANDS_TO_NGPS_TARGET = {}
for k, v in NGPS_TARGET_BANDS_TO_SNCOSMO.items():
    for vv in v:
        SNCOSMO_BANDS_TO_NGPS_TARGET[vv] = k

ALL_NGPS_SNCOSMO_BANDS = []
for v in NGPS_TARGET_BANDS_TO_SNCOSMO.values():
    ALL_NGPS_SNCOSMO_BANDS.extend(v)

gaia = GaiaQuery()


def warningfilter(action="ignore", category=RuntimeWarning):
    """decorator to filter warnings using a context manager"""

    def wrapper(func):
        @wraps(func)
        def decorated_function(*args, **kwargs):
            with warnings.catch_warnings():
                warnings.simplefilter(action, category=category)
                return func(*args, **kwargs)

        return decorated_function

    return wrapper


def format_hmsdms(skycoord, coord_sep, col_sep):
    """Format a SkyCoord object as a string in HMSDMS format"""
    hmsdms = skycoord.to_string(
        "hmsdms", sep=":", decimal=False, precision=2, alwayssign=True
    )

    if isinstance(hmsdms, list) and len(hmsdms) > 1:
        output = []
        for x in hmsdms:
            ra, dec = x.split(" ")
            output.append(
                ra.replace(":", coord_sep) + col_sep + dec.replace(":", coord_sep)
            )
        return output

    ra, dec = hmsdms[1:].split(" ")
    output = ra.replace(":", coord_sep) + col_sep + dec.replace(":", coord_sep)
    return output


facility_parameters = {
    "Keck": {
        "radius_degrees": 2.0 / 60,
        "mag_limit": 18.5,
        "mag_min": 11.0,
        "min_sep_arcsec": 4.0,
    },
    "P200": {
        "radius_degrees": 2.0 / 60,
        "mag_limit": 18.0,
        "mag_min": 10.0,
        "min_sep_arcsec": 5.0,
    },
    "P200-NGPS": {
        "radius_degrees": 2.0 / 60,
        "mag_limit": 18.0,
        "mag_min": 10.0,
        "min_sep_arcsec": 5.0,
    },
    "Shane": {
        "radius_degrees": 2.5 / 60,
        "mag_limit": 17.0,
        "mag_min": 10.0,
        "min_sep_arcsec": 5.0,
    },
}

# ZTF ref grabber URLs. See get_ztfref_url() below
irsa = {
    "url_data": "https://irsa.ipac.caltech.edu/ibe/data/ztf/products/",
    "url_search": "https://irsa.ipac.caltech.edu/ibe/search/ztf/products/",
}


starlist_formats = {
    "Keck": {
        "coord_sep": " ",
        "col_sep": " ",
        "commentstr": "#",
        "giveoffsets": True,
        "maxname_size": 15,
        "first_line": None,
    },
    "P200": {
        "coord_sep": " ",
        "col_sep": "  ",
        "commentstr": "!",
        "giveoffsets": False,
        "maxname_size": 18,
        "first_line": None,
    },
    "P200-NGPS": {
        "coord_sep": ":",
        "col_sep": ",",
        "commentstr": "!",
        "giveoffsets": False,
        "maxname_size": 18,
        "first_line": None,
    },
    "Shane": {
        "coord_sep": " ",
        "col_sep": " ",
        "commentstr": "#",
        "giveoffsets": True,
        "maxname_size": 15,
        # see https://mthamilton.ucolick.org/techdocs/telescopes/Shane/coords/
        "first_line": (
            "!Data {name %16} ra_h ra_m ra_s dec_d dec_m dec_s "
            "equinox keyval {comment *}"
        ),
    },
}

JOBLIB_CACHE_SIZE = 100e6  # 100 MB
offsets_memory = Memory("./cache/offsets/", verbose=0)


def memcache(f):
    """Ensure that joblib memory cache stays within bytes limit."""
    offsets_memory.reduce_size(JOBLIB_CACHE_SIZE)
    return offsets_memory.cache(f)


def get_url(*args, **kwargs):
    # Connect and read timeouts
    kwargs["timeout"] = (6.05, 20)
    try:
        return requests.get(*args, **kwargs)
    except requests.exceptions.RequestException:
        return None


@memcache
def get_ps1_url(ra, dec, imsize, *args, **kwargs):
    """
    Returns the URL that points to the PS1 image for the
    requested position

    Parameters
    ----------
    ra : float
        Right ascension (J2000) of the source
    dec : float
        Declination (J2000) of the source
    imsize : float
        Requested image size (on a size) in arcmin
    *args : optional
        Extra args (not needed here)
    **kwargs : optional
        Extra kwargs (not needed here)

    Returns
    -------
    str
        the URL to download the PS1 image

    """
    # calculate number of PS1 pixels given the 0.25"/pix scale
    numpix = math.ceil(60 * imsize / 0.25)

    ps_query_url = (
        f"http://ps1images.stsci.edu/cgi-bin/ps1cutouts"
        f"?pos={ra}+{dec}&"
        f"filter=r&filetypes=stack&size={numpix}"
    )

    try:
        response = requests.get(ps_query_url, timeout=PS1_CUTOUT_TIMEOUT)
        # see models.py for how this URL is constructed
        match = re.search('src="//ps1images.stsci.edu.*?"', response.content.decode())
        if match is None:
            log(f"PS1 image not found for {ra} {dec}")
            return ""
        url = match.group().replace('src="', "http:").replace('"', "")
        url += f"&format=fits&imagename=ps1{ra}{dec:+f}.fits"
    except (requests.exceptions.SSLError, requests.exceptions.ReadTimeout) as e:
        log(f"Error getting PS1 image URL {str(e)}")
        return ""
    except Exception as e:
        log(f"Error getting PS1 image URL {e.message}")
        return ""

    return url


@memcache
def get_ps1_cds_url(ra, dec, imsize, *args, **kwargs):
    """
    Returns the URL that points to the PS1 image for the
    requested position, using CDS service
    """

    fov = imsize / 60.0  # from arcmin to degrees
    url = (
        f"https://alasky.cds.unistra.fr/hips-image-services/hips2fits"
        f"?width=500&height=500&fov={fov}&ra={ra}&dec={dec}"
        f"&hips=CDS/P/PanSTARRS/DR1/r"
    )
    return url


@memcache
def get_ztfref_url(ra, dec, imsize, *args, **kwargs):
    """
    From:
    https://gist.github.com/dmitryduev/634bd2b21a77e2b1de89e0bfd39d14b9

    Returns the URL that points to the ZTF reference image for the
    requested position

    Parameters
    ----------
    ra : float
        Right ascension (J2000) of the source
    dec : float
        Declination (J2000) of the source
    imsize : float
        Requested image size (on a size) in arcmin
    *args : optional
        Extra args (not needed here)
    **kwargs : optional
        Extra kwargs (not needed here)

    Returns
    -------
    str
        the URL to download the ZTF image

    """
    imsize_deg = imsize / 60

    url_ref_meta = os.path.join(
        irsa["url_search"], f"ref?POS={ra:f},{dec:f}&SIZE={imsize_deg:f}&ct=csv"
    )
    r = get_url(url_ref_meta)
    if r is None:
        return ""
    s = r.content
    c = pd.read_csv(io.StringIO(s.decode("utf-8")))

    try:
        field = f"{c.loc[0, 'field']:06d}"
        filt = c.loc[0, "filtercode"]
        quad = f"{c.loc[0, 'qid']}"
        ccd = f"{c.loc[0, 'ccdid']:02d}"
    except KeyError:
        log(f"Note: ZTF does not have a reference image at the position {ra} {dec}")
        return ""

    path_ursa_ref = os.path.join(
        irsa["url_data"],
        "ref",
        field[:3],
        f"field{field}",
        filt,
        f"ccd{ccd}",
        f"q{quad}",
        f"ztf_{field}_{filt}_c{ccd}_q{quad}_refimg.fits",
    )
    return path_ursa_ref


def ngps_defaults(mag, magfilter):
    try:  # if numerical, format to 2 decimal places
        mag = f"{mag:<0.02f}"
    except (TypeError, ValueError):
        pass
    return f"2,3,PA,1.5,2.5,650,680,R,{mag},{magfilter},SNR 5"


# helper dict for seaching for FITS images from various surveys
source_image_parameters = {
    "desi": {
        "url": (
            "http://legacysurvey.org/viewer/fits-cutout/"
            "?ra={ra}&dec={dec}&layer=dr8&pixscale={pixscale}&bands=r"
        ),
        "npixels": 256,
        "smooth": None,
        "str": "DESI DR8 R-band",
    },
    "dss": {
        "url": (
            "http://archive.stsci.edu/cgi-bin/dss_search"
            "?v=poss2ukstu_red&r={ra}&dec={dec}&h={imsize}&w={imsize}&e=J2000"
        ),
        "smooth": None,
        "reproject": True,
        "npixels": 500,
        "str": "DSS-2 Red",
    },
    "ztfref": {
        "url": get_ztfref_url,
        "reproject": True,
        "npixels": 500,
        "smooth": None,
        "str": "ZTF Ref",
    },
    "ps1": {
        "url": get_ps1_url,
        "reproject": True,
        "npixels": 500,
        "smooth": None,
        "str": "PS1 r-band",
    },
    "ps1_cds": {
        "url": get_ps1_cds_url,
        "reproject": True,
        "npixels": 500,
        "smooth": None,
        "str": "PS1 r-band (CDS)",
    },
}


def get_astrometry_backup_from_ztf(
    ra,
    dec,
    max_offset_arcsec=600,
    extra_backup_ztf_columns={
        "ref_epoch": (2015.5, u.year),
        "pmra": (0.0, u.mas / u.year),
        "pmdec": (0.0, u.mas / u.year),
        "parallax": (0.1, u.mas),
    },
):
    """Get astrometry from ZTF, making the result look like a Gaia query result.

    Parameters
    ----------
    ra : float
        Right ascension (J2015.5) of the source
    dec : float
        Declination (J2015.5) of the source
    extra_backup_ztf_columns : dictionary, optional
        Extra columns to add to the astrometry table, along
        with the default and relevant units.

    Returns
    -------
    astropy.table.Table
        Astrometry table

    """
    # get the ZTF catalog data and make it look like a Gaia Query result
    ztf_astrometry = get_ztfcatalog(ra, dec, as_astropy_table=True)
    if len(ztf_astrometry) == 0:
        return ztf_astrometry

    ztf_astrometry.rename_column("sourceid", "source_id")
    ztf_astrometry.rename_column("mag", "phot_rp_mean_mag")
    ztf_astrometry["phot_rp_mean_mag"].fill_value = 20.0
    ztf_astrometry["phot_rp_mean_mag"].unit = u.mag

    ztf_astrometry.remove_columns(
        ["xpos", "ypos", "flux", "sigflux", "sigmag", "snr", "chi", "sharp", "flags"]
    )

    catalog = SkyCoord.guess_from_table(ztf_astrometry)
    center = SkyCoord(
        ra=ra,
        dec=dec,
        unit=(u.degree, u.degree),
        pm_ra_cosdec=0 * u.mas / u.yr,
        pm_dec=0 * u.mas / u.yr,
        frame="icrs",
        distance=10 * u.kpc,
        obstime=Time(
            extra_backup_ztf_columns.get("ref_epoch", (2015.5,))[0],
            format="decimalyear",
        ),
    )
    ztf_astrometry["dist"] = center.separation(catalog).degree
    ztf_astrometry["dist"].unit = u.degree
    filter_mask = ztf_astrometry["dist"] <= max_offset_arcsec * u.arcsec
    ztf_astrometry = ztf_astrometry[filter_mask]

    if len(ztf_astrometry) == 0:
        return ztf_astrometry

    # add the extra columns
    for k, v in extra_backup_ztf_columns.items():
        ztf_astrometry[k] = v[0]
        if v[1] is not None:
            ztf_astrometry[k].unit = v[1]

    return ztf_astrometry


@memcache
def get_ztfcatalog(
    ra,
    dec,
    cache_dir="./cache/finder_cat/",
    cache_max_items=1000,
    as_astropy_table=False,
):
    """Finds the ZTF public catalog data around this position

    Parameters
    ----------
    ra : float
        Right ascension (J2000) of the source
    dec : float
        Declination (J2000) of the source
    cache_dir : str, optional
        Directory to cache the astrometry data
    cache_max_items : int, optional
        How many files to keep in the cache
    as_astropy_table : bool, optional
        If True, return the data as an astropy table
        If False, return the data as a SkyCoord list
    """
    cache = Cache(cache_dir=cache_dir, max_items=cache_max_items)

    refurl = get_ztfref_url(ra, dec, imsize=5)
    if refurl is None or refurl == "":
        log("Empty ZTF reference image URL. Returning empty table.")
        return Table()

    # the catalog data is in the same directory as the reference images
    caturl = refurl.replace("_refimg.fits", "_refpsfcat.fits")
    catname = os.path.basename(caturl)
    hdu_fn = cache[catname]

    if hdu_fn is not None:
        with fits.open(hdu_fn) as hdu:
            data = hdu[1].data
    else:
        response = get_url(caturl, stream=True, allow_redirects=True)
        if response is None or response.status_code != 200:
            return None
        else:
            with fits.open(io.BytesIO(response.content)) as hdu:
                buf = io.BytesIO()
                hdu.writeto(buf)
                buf.seek(0)
                cache[catname] = buf.read()
                data = hdu[1].data

    ztftable = Table(data)
    ztftable["ra"].unit = u.deg
    ztftable["dec"].unit = u.deg
    if as_astropy_table:
        try:
            magzp = float(hdu[0].header["MAGZP"])
        except KeyError:
            magzp = 25.0
        ztftable["mag"] += magzp
        return ztftable
    try:
        catalog = SkyCoord.guess_from_table(ztftable)
        return catalog
    except ValueError:
        return Table()


@warningfilter(action="ignore", category=RuntimeWarning)
def _calculate_best_position_for_offset_stars(
    photometry, fallback=(None, None), how="snr2", max_offset=0.5, sigma_clip=4.0
):
    """Calculates the best position for a source from its photometric
       points. Only small adjustments from the fallback position are
       expected.

    Parameters
    ----------
    photometry : list
        List of Photometry objects
    fallback : tuple, optional
        The position to use if something goes wrong here
    how : str
        how to weight positional data:
          snr2 = use the signal to noise squared
          invvar = use the inverse photometric variance
    max_offset : float, optional
        How many arcseconds away should we ignore discrepant points?
    sigma_clip : float, optional
        Remove positions that are this number of std away from the median
    """
    if not isinstance(photometry, list):
        log("Warning: No photometry given. Falling back to original source position.")
        return fallback

    # convert the photometry into a dataframe
    phot = [x.to_dict() for x in photometry]
    df = pd.DataFrame(phot)

    # remove limit data (non-detections)
    try:
        df = df[(df["flux"].notnull()) & (df["fluxerr"].notnull())]
    except KeyError:
        log(
            "Photometry does not include fluxes. Falling back to "
            " original source position."
        )
        return fallback

    # remove observations with distances more than max_offset away
    # from the median
    try:
        # use nanmedian so that med_ra, med_dec are not returned as
        # nan when df['ra'] or df['dec'] contains `None`s (can happen
        # when there is no position information for a photometry
        # point)
        med_ra, med_dec = np.nanmedian(df["ra"]), np.nanmedian(df["dec"])
    except (TypeError, AttributeError):
        log(
            "Warning: could not find the median of the positions"
            " from the photometry data associated with this source "
        )
        return fallback
    except Exception as e:
        log(e)
        log("Uncaught error in ra, dec determination")
        return fallback

    if np.isnan(med_ra) or np.isnan(med_dec):
        log(
            "Warning: the median of the positions"
            " from the photometry data associated with this source "
            " retured nan, using fallback"
        )
        return fallback

    # check to make sure that the median isn't too far away from the
    # discovery position
    if fallback != (None, None):
        c1 = SkyCoord(med_ra * u.deg, med_dec * u.deg, frame="icrs")
        c2 = SkyCoord(fallback[0] * u.deg, fallback[1] * u.deg, frame="icrs")
        sep = c1.separation(c2)
        if np.abs(sep) > max_offset * u.arcsec:
            log(
                "Warning: calculated source position is too far from the"
                " fiducial. Falling back to the fiducial "
            )
            return fallback

    df["ra_offset"] = np.cos(np.radians(df["dec"])) * (df["ra"] - med_ra) * 3600.0
    df["dec_offset"] = (df["dec"] - med_dec) * 3600.0
    df["offset_arcsec"] = np.sqrt(df["ra_offset"] ** 2 + df["dec_offset"] ** 2)
    df = df[df["offset_arcsec"] <= max_offset]

    # remove outliers
    if len(df) > 4 and sigma_clip is not None:
        df = df[df["offset_arcsec"] < sigma_clip * np.std(df["offset_arcsec"])]

    # TODO: add the ability to use only use observations from some filters
    try:
        if how == "snr2":
            df["snr"] = df["flux"] / df["fluxerr"]
            diff_ra = np.average(df["ra_offset"], weights=df["snr"] ** 2)
            diff_dec = np.average(df["dec_offset"], weights=df["snr"] ** 2)
        elif how == "invvar":
            diff_ra = np.average(df["ra_offset"], weights=1 / df["ra_unc"] ** 2)
            diff_dec = np.average(df["dec_offset"], weights=1 / df["dec_unc"] ** 2)
        else:
            log(f"Warning: do not recognize {how} as a valid way to weight astrometry")
            return (med_ra, med_dec)
    except ZeroDivisionError as e:
        log(f"ZeroDivisionError in calculating position with {how}: {e}")
        return (med_ra, med_dec)

    if not np.isfinite([diff_ra, diff_dec]).all():
        log(f"Error calculating position correction with {how}: {[diff_ra, diff_dec]}")
        return (med_ra, med_dec)

    position = (
        med_ra + diff_ra / (np.cos(np.radians(med_dec)) * 3600.0),
        med_dec + diff_dec / 3600.0,
    )
    return position


def get_formatted_standards_list(
    starlist_type="Keck",
    standard_type="ESO",
    dec_filter_range=(-90, 90),
    ra_filter_range=(0, 360),
    magnitude_range=(np.inf, -np.inf),
    show_first_line=False,
    return_dataframe=False,
):
    """Returns a list of standard stars in the preferred starlist format.

    The standards collections are established in the config.yaml file
    pointing to a CSV file with the standards

    Parameters
    ----------
    starlist_type : str, optional
        Type of starlist (Keck, P200, P200-NGPS, Shane)
    standard_type : str, optional
        Name of the collection of standards (ESO, ZTF, ...)
    dec_filter_range: tuple, optional
        Inclusive range in degrees to keep standards. Useful for showing
        fewer sources which are not accessible to a given telescope
    ra_filter_range: tuple, optional
        Inclusive range in degrees to keep standards. Useful for showing
        fewer sources which are not accessible to a given telescope. If
        the first element is larger than the first, then assume the filter
        request wraps around 0. Ie. if ra_filter_range = (150, 20) then
        include all sources with ra > 150 or ra < 20.
    magnitude_range : tuple, optional
        Restrict magnitude range of standards.
    show_first_line: bool, optional
        Return the first formatting line  if the starlist type adds
        a preamble
    return_dataframe: bool, optional
        Return star list as a dataframe
    """
    starlist = []
    result = {"starlist_info": starlist, "success": False}
    standard_stars = cfg["standard_stars"]

    standard_file = standard_stars.get(standard_type)
    if standard_file is None:
        log(f"Warning: '{standard_type}' not defined in the config.yaml.")
        return result

    starlist_format = starlist_formats.get(starlist_type)
    if starlist_format is None:
        log("Warning: Do not recognize this starlist format. Using Keck.")
        starlist_format = starlist_formats["Keck"]

    space = " "
    col_sep = starlist_format["col_sep"]
    coord_sep = starlist_format["coord_sep"]
    commentstr = starlist_format["commentstr"]
    maxname_size = starlist_format["maxname_size"]
    if show_first_line and starlist_format["first_line"] not in [None, ""]:
        starlist.append(starlist_format["first_line"])

    df = pd.read_csv(standard_file, comment="#")
    if not {"name", "ra", "dec", "epoch", "comment"}.issubset(set(df.columns.values)):
        log("Error: Standard star CSV file is missing necessary headers.")
        return result

    tab = SkyCoord(df["ra"], df["dec"], unit=(u.hourangle, u.deg))
    df["ra_float"] = tab.ra.value
    df["dec_float"] = tab.dec.value
    df["skycoord"] = [x[1:] for x in format_hmsdms(tab, coord_sep, col_sep)]

    # filter
    df = df[
        (df["dec_float"] >= dec_filter_range[0])
        & (df["dec_float"] <= dec_filter_range[1])
    ]
    if ra_filter_range[1] > ra_filter_range[0]:
        df = df[
            (df["ra_float"] >= ra_filter_range[0])
            & (df["ra_float"] <= ra_filter_range[1])
        ]
    else:
        df = df[
            (df["ra_float"] >= ra_filter_range[0])
            | (df["ra_float"] <= ra_filter_range[1])
        ]

    if standard_type == "ESO":
        mag = []
        for _, row in df.iterrows():
            commentSplit = row["comment"].split(" ")
            mag.append(float(commentSplit[1].replace("V=", "")))
        df["mag"] = mag

        df = df[(df["mag"] <= magnitude_range[0]) & (df["mag"] >= magnitude_range[1])]

    if len(df) == 0:
        log("Warning: No standards stars match the filter criteria.")
        return result

    if return_dataframe:
        return df
    elif starlist_type == "P200-NGPS":
        # special format for NGPS, CSV-like with additional columns
        for _, row in df.iterrows():
            if "mag" in df.columns:
                mag, magfilter = row["mag"], "V"
            else:
                mag, magfilter = "", ""
            starlist.append(
                {
                    "str": (
                        f"{row['name']}"
                        + ","
                        + f"{row['skycoord']}"
                        + ",,"  # offset ra, dec, empty for standards
                        + ","
                        + "standard"  # comment
                        + ","  # priority, empty for standards
                        + ","
                        + ngps_defaults(mag, magfilter)
                    )
                }
            )
        return {"starlist_info": starlist, "success": True}
    else:
        for index, row in df.iterrows():
            starlist.append(
                {
                    "str": (
                        f"{row['name'].replace(' ', ''):{space}<{maxname_size}}"
                        + col_sep
                        + f"{row.skycoord}"
                        + col_sep
                        + f"{row.epoch}"
                        + col_sep
                        + f"{commentstr} {row.comment}"
                    )
                }
            )
        return {"starlist_info": starlist, "success": True}


@warningfilter(action="ignore", category=DeprecationWarning)
@warningfilter(action="ignore", category=AstropyWarning)
@memcache
def get_nearby_offset_stars(
    source_ra,
    source_dec,
    source_name,
    how_many=3,
    radius_degrees=2 / 60.0,
    mag_limit=18.0,
    mag_min=10.0,
    min_sep_arcsec=2,
    starlist_type="Keck",
    obstime=None,
    use_source_pos_in_starlist=True,
    allowed_queries=2,
    queries_issued=0,
    use_ztfref=True,
    use_ztfref_as_gaia_backup=True,
    required_ztfref_source_distance=60,
    assignment_priority=1,
    assignment_comment="science",
    source_mag=None,
    source_magfilter=None,
):
    """Finds good list of nearby offset stars for spectroscopy
       and returns info about those stars, including their
       offsets calculated to the source of interest

    Parameters
    ----------
    source_ra : float
        Right ascension (J2000) of the source
    source_dec : float
        Declination (J2000) of the source
    source_name : str
        Name of the source
    how_many : int, optional
        How many offset stars to try to find
    radius_degrees : float, optional
        Search radius from the source position in arcmin
    mag_limit : float, optional
        How faint should we search for offset stars?
    mag_min : float, optional
        What is the brightest offset star we will allow?
    min_sep_arcsec : float, optional
        What is the closest offset star allowed to the source?
    starlist_type : str, optional
        What starlist format should we use?
    obstime : str, optional
        What datetime (in isoformat) should we assume for the observation
        (to calculate proper motions)?
    use_source_pos_in_starlist : bool, optional
        Return the source itself for in starlist?
    allowed_queries : int, optional
        How many times should we query (with looser and looser criteria)
        before giving up on getting the number of offset stars we desire?
    queries_issued : int, optional
        How many times have we issued a query? Bookkeeping parameter.
    use_ztfref : boolean, optional
        Use the ZTFref catalog for offset star positions if possible
    use_ztfref_as_gaia_backup : boolean, optional
        Use the ZTFref catalog for finding the initial offset star positions
        if Gaia fails to return any stars. This is useful for the case where
        Gaia servers are down.
    required_ztfref_source_distance : float, optional
        If there are zero ZTF ref stars within this distance in arcsec,
        then do not use the ztfref catalog even if asked. This probably
        means that the source is at the end of the ref catalog.
    assignment_priority : int, optional
        Priority of the optional observing run assignment, if the starlist format supports it
    assignment_comment : str, optional
        Comment for the optional observing run assignment, if the starlist format supports it
    source_mag : float, optional
        Magnitude of the source
    source_magfilter : str, optional
        Filter of the source magnitude

    Returns
    -------
    (list, str, int, int, bool)
        Return a tuple which contains: a list of dictionaries for each object
        in the star list, the query issued, the number of queries issues,
        the length of the star list (not including the source itself),
        and whether the ZTFref catalog was used for source positions or not.
    """
    if queries_issued >= allowed_queries:
        raise Exception("Number of offsets queries needed exceeds what is allowed")

    if not obstime:
        source_obstime = Time(datetime.datetime.utcnow().isoformat())
    else:
        # TODO: check the obstime format
        source_obstime = Time(obstime)

    center = SkyCoord(
        source_ra,
        source_dec,
        unit=(u.degree, u.degree),
        frame="icrs",
        obstime=source_obstime,
    )
    # get three times as many stars as requested for now
    # and go fainter as well
    fainter_diff = 1.5  # mag
    search_multipler = 20
    min_distance = 5.0 / 3600.0  # min distance from source for offset star
    source_in_catalog_dist = 0.5 / 3600.0  # min distance from source for offset star
    query_string = f"""
                  SELECT DISTANCE(
                    POINT('ICRS', ra, dec),
                    POINT('ICRS', {source_ra}, {source_dec})) AS
                    dist, source_id, ra, dec, ref_epoch,
                    phot_rp_mean_mag, pmra, pmdec, parallax
                  FROM {{main_db}}.gaia_source
                  WHERE 1=CONTAINS(
                    POINT('ICRS', ra, dec),
                    CIRCLE('ICRS', {source_ra}, {source_dec},
                           {radius_degrees}))
                """
    default_return = (
        [],
        query_string.replace("\n", " "),
        queries_issued,
        0,
        False,
    )

    # try to get Gaia sources first
    with gaia as g:
        try:
            r = g.query(query_string)
        except Exception as e:
            log(f"Error querying Gaia: {e}. Falling back to ZTFref or empty result.")
            r = None

    # ...otherwise fall back to ZTFref public sources or return
    # a tuple of no offset stars
    if r is None:
        if use_ztfref_as_gaia_backup:
            r = get_astrometry_backup_from_ztf(source_ra, source_dec)
            use_ztfref = True
        else:
            return default_return
    if r is None or len(r) == 0:
        return default_return

    # we need to filter here to get around the new Gaia archive slowdown
    # when SQL filtering on different columns
    filter_mask = (
        (r["phot_rp_mean_mag"] < mag_limit + fainter_diff)
        & (r["phot_rp_mean_mag"] > mag_min)
        & (r["parallax"] < 250)
    )
    r = r[filter_mask]
    # sort by distance and take the top several results
    # we need to do this here because gaia ADQL sort is not working
    r.sort("dist")
    r = r[: int(how_many * search_multipler)]

    # get brighter stars at top for the nearby sources:
    r.sort("phot_rp_mean_mag")
    potential_source_in_gaia_query = r[r["dist"] < source_in_catalog_dist]
    if len(potential_source_in_gaia_query) > 0:
        # try to find offset stars brighter than the catalog brightness of the
        # source.
        source_catalog_mag = potential_source_in_gaia_query["phot_rp_mean_mag"]
        offset_brightness_limit = source_catalog_mag
        for _ in range(3):
            temp_r = r[r["phot_rp_mean_mag"] <= offset_brightness_limit]
            if len(temp_r) > how_many + 2:
                r = temp_r
                break
            offset_brightness_limit += 0.5

    # filter out stars near the source (and the source itself)
    # since we do not want waste an offset star on very nearby sources
    r = r[r["dist"] > min_distance]

    queries_issued += 1

    catalog = SkyCoord.guess_from_table(r)
    if use_ztfref:
        ztfcatalog = get_ztfcatalog(source_ra, source_dec)
        if ztfcatalog is None or len(ztfcatalog) == 0:
            log(
                "Warning: Could not find the ZTF reference catalog"
                f" at position {source_ra} {source_dec}"
            )
        else:
            if (
                sum(
                    center.separation(ztfcatalog)
                    < required_ztfref_source_distance * u.arcsec
                )
                == 0
            ):
                ztfcatalog = None
                log(
                    "Warning: The ZTF reference catalog is empty near"
                    f" position {source_ra} {source_dec}. This probably means"
                    " that the source is at the edge of the ref catalog."
                )
                use_ztfref = False

    # star needs to be this far away
    # from another star
    min_sep = min_sep_arcsec * u.arcsec
    good_list = []
    for source in r:
        c = SkyCoord(
            ra=source["ra"],
            dec=source["dec"],
            unit=(u.degree, u.degree),
            pm_ra_cosdec=source["pmra"] * u.mas / u.yr,
            pm_dec=source["pmdec"] * u.mas / u.yr,
            frame="icrs",
            distance=min(abs(1 / source["parallax"]), 10) * u.kpc,
            obstime=Time(source["ref_epoch"], format="jyear"),
        )

        d2d = c.separation(catalog)  # match it to the catalog
        if sum(d2d < min_sep) == 1 and source["phot_rp_mean_mag"] <= mag_limit:
            use_original = True

            # this star is not near another star and is bright enough

            # if there's a close match to ZTF reference position then use
            #  ZTF position for this source instead of the gaia/motion data
            if use_ztfref and ztfcatalog is not None:
                try:
                    idx, ztfdist, _ = c.match_to_catalog_sky(ztfcatalog)

                    if ztfdist < 0.5 * u.arcsec:
                        cprime = SkyCoord(
                            ra=ztfcatalog[idx].ra.value,
                            dec=ztfcatalog[idx].dec.value,
                            unit=(u.degree, u.degree),
                            frame="icrs",
                            obstime=source_obstime,
                        )

                        dra, ddec = cprime.spherical_offsets_to(center)
                        pa = cprime.position_angle(center).degree
                        # use the RA, DEC from ZTF here
                        source["ra"] = ztfcatalog[idx].ra.value
                        source["dec"] = ztfcatalog[idx].dec.value
                        good_list.append(
                            (
                                source["dist"],
                                cprime,
                                source,
                                dra.to(u.arcsec),
                                ddec.to(u.arcsec),
                                pa,
                            )
                        )
                        use_original = False
                except Exception as e:
                    log(
                        f"Warning: ZTF catalog matching failed... "
                        f"Error: str{e} "
                        f"Failed catalog: {str(ztfcatalog)}"
                    )

            if use_original:
                # precess it's position forward to the source obstime and
                # get offsets suitable for spectroscopy
                # TODO: put this in geocentric coords to account for parallax
                cprime = c.apply_space_motion(new_obstime=source_obstime)
                dra, ddec = cprime.spherical_offsets_to(center)
                pa = cprime.position_angle(center).degree
                good_list.append(
                    (
                        source["dist"],
                        cprime,
                        source,
                        dra.to(u.arcsec),
                        ddec.to(u.arcsec),
                        pa,
                    )
                )

    good_list.sort()

    # if we got less than we asked for, relax the criteria
    if (len(good_list) < how_many) and (queries_issued < allowed_queries):
        return get_nearby_offset_stars(
            source_ra,
            source_dec,
            source_name,
            how_many=how_many,
            radius_degrees=radius_degrees * 1.3,
            mag_limit=mag_limit + 1.0,
            mag_min=mag_min - 1.0,
            min_sep_arcsec=min_sep_arcsec / 2.0,
            starlist_type=starlist_type,
            obstime=obstime,
            use_source_pos_in_starlist=use_source_pos_in_starlist,
            queries_issued=queries_issued,
            allowed_queries=allowed_queries,
            use_ztfref=use_ztfref,
            required_ztfref_source_distance=required_ztfref_source_distance,
            assignment_priority=assignment_priority,
            assignment_comment=assignment_comment,
            source_mag=source_mag,
            source_magfilter=source_magfilter,
        )

    starlist_format = starlist_formats.get(starlist_type)
    if starlist_format is None:
        log("Warning: Do not recognize this starlist format. Using Keck.")
        starlist_format = starlist_formats["Keck"]

    col_sep = starlist_format["col_sep"]
    coord_sep = starlist_format["coord_sep"]
    commentstr = starlist_format["commentstr"]
    giveoffsets = starlist_format["giveoffsets"]
    maxname_size = starlist_format["maxname_size"]
    first_line = starlist_format["first_line"]

    basename = source_name.strip().replace(" ", "")
    if len(basename) > maxname_size:
        basename = basename[3:]

    abrev_basename = source_name.strip().replace(" ", "")
    if len(abrev_basename) > maxname_size - 3:
        abrev_basename = basename[3:maxname_size]

    space = " "
    hmsdms = format_hmsdms(center, coord_sep, col_sep)
    if starlist_type == "P200-NGPS":
        # special format for NGPS, CSV-like with additional columns
        if not source_mag or not source_magfilter:
            mag, magfilter = "", ""
        else:
            magfilter = SNCOSMO_BANDS_TO_NGPS_TARGET.get(source_magfilter, None)
            if magfilter is None:
                raise ValueError(
                    f"Cannot find corresponding NGPS filter for sncosmo filter {magfilter}"
                )
            try:
                mag = round(float(source_mag), 2)
            except ValueError:
                raise ValueError(f"Cannot convert magnitude {source_mag} to float")

        # we only keep letters, numbers, special characters but remove all \n, \t, ... and other control characters
        # we also remove all mentions of the col_sep character used by the starlist format
        assignment_comment = str(assignment_comment)
        assignment_comment = "".join(
            [
                c
                for c in assignment_comment
                if c.isalnum() or c in string.punctuation or c == " "
            ]
        )
        assignment_comment = assignment_comment.replace(col_sep, " ")

        star_list_format = (
            f"{basename}"
            + ","
            + f"{hmsdms}"
            + ",,"  # offset ra, dec (empty for target)
            + ","
            + assignment_comment
            + ","
            + str(int(assignment_priority))  # assignment priority, if any
            + ","
            + ngps_defaults(mag, magfilter)
        )
    else:
        star_list_format = (
            f"{basename:{space}<{maxname_size}}"
            + col_sep
            + f"{hmsdms}"
            + col_sep
            + "2000.0"
            + col_sep
            + f"{commentstr} source_name={source_name}"
        )

    star_list = [{"str": first_line}] if first_line else []
    if use_source_pos_in_starlist:
        star_list.append(
            {
                "str": star_list_format,
                "ra": float(source_ra),
                "dec": float(source_dec),
                "name": basename,
            }
        )

    for i, (dist, c, source, dra, ddec, pa) in enumerate(good_list[:how_many]):
        dras = f'{dra.value:<0.03f}" E' if dra > 0 else f'{abs(dra.value):<0.03f}" W'
        ddecs = (
            f'{ddec.value:<0.03f}" N' if ddec > 0 else f'{abs(ddec.value):<0.03f}" S'
        )

        if giveoffsets:
            offsets = f"raoffset={dra.value:<0.03f} decoffset={ddec.value:<0.03f}"
        else:
            offsets = ""

        if starlist_type == "P200-NGPS":
            name = f"{abrev_basename}_o{i + 1}"
        else:
            name = f"{abrev_basename}_{starlist_type.lower()[0]}{i + 1}"

        hmsdms = format_hmsdms(c, coord_sep, col_sep)

        # the id_col isn't necessarily source_id, it might be SOURCE_ID, so figure out which one it is:
        id_col = "source_id"
        for k in list(source.keys()):
            if "source_id" in str(k).lower().strip():
                id_col = k
                break

        if starlist_type == "P200-NGPS":
            # special format for NGPS, CSV-like with additional columns
            star_list_format = (
                f"{name}"
                + ","
                + f"{hmsdms}"
                + f",{dra.value:<0.03f},{ddec.value:<0.03f},"  # offset ra, dec
                + "offset"  # comment
                + ","  # priority (empty for offsets)
                + ","
                + ngps_defaults(source["phot_rp_mean_mag"], "R")
            )
        else:
            star_list_format = (
                f"{name:{space}<{maxname_size}}"
                + col_sep
                + f"{hmsdms}"
                + col_sep
                + "2000.0"
                + col_sep
                + f"{offsets}"
                + f"{col_sep if giveoffsets else ''}"
                + f'{commentstr} dist={3600 * dist:<0.02f}"; {source["phot_rp_mean_mag"]:<0.02f} mag'
                + f"; {dras}, {ddecs} PA={pa:<0.02f} deg"
                + f" ID={source[id_col]}"
            )

        star_list.append(
            {
                "str": star_list_format,
                "ra": c.ra.value,
                "dec": c.dec.value,
                "name": name,
                "dras": dras,
                "ddecs": ddecs,
                "mag": float(source["phot_rp_mean_mag"]),
                "pa": pa,
            }
        )

    # send back the starlist in
    return (
        star_list,
        query_string.replace("\n", " "),
        queries_issued,
        len(star_list) - 1,
        use_ztfref,
    )


def fits_image(
    center_ra,
    center_dec,
    imsize=4.0,
    image_source="ps1",
    cache_dir="./cache/finder/",
    cache_max_items=1000,
):
    """Returns an opened FITS image centered on the source
       of the requested size.

    Parameters
    ----------
    source_ra : float
        Right ascension (J2000) of the source
    source_dec : float
        Declination (J2000) of the source
    imsize : float, optional
        Requested image size (on a size) in arcmin
    image_source : str, optional
        Survey where the image comes from "desi", "dss", "ztfref", "ps1"
    cache_dir : str, optional
        Where should the cache live?
    cache_max_items : int, optional
        How many older files in the cache should we keep?  Set to zero
        to disable cache.

    Returns
    -------
    object
        Either a pyfits HDU object or None. If no suitable image is found
        then None is returned. The caller of `fits_image` will need to
        handle this case.
    """

    if image_source not in source_image_parameters:
        raise Exception("do not know how to grab image source")

    pixscale = 60 * imsize / source_image_parameters[image_source].get("npixels", 256)

    if isinstance(source_image_parameters[image_source]["url"], str):
        url = source_image_parameters[image_source]["url"].format(
            ra=center_ra, dec=center_dec, pixscale=pixscale, imsize=imsize
        )
    else:
        # use the URL field as a function
        url = source_image_parameters[image_source]["url"](
            ra=center_ra, dec=center_dec, imsize=imsize
        )

    if url in [None, ""]:
        log(f"Could not get FITS image for source {image_source}")
        return None

    cache = Cache(cache_dir=cache_dir, max_items=cache_max_items)

    def get_hdu(url):
        """Try to get HDU from cache, otherwise fetch."""
        hash_name = f"{center_ra}{center_dec}{imsize}{image_source}"
        hdu_fn = cache[hash_name]

        # Found entry in cache, return that
        if hdu_fn is not None:
            return fits.open(hdu_fn)[0]

        response = get_url(url, stream=True, allow_redirects=True)
        if response is None or response.status_code != 200:
            return None

        # Check if HDU is a valid FITS file
        hdu = fits.open(io.BytesIO(response.content))[0]

        # Ensure it is not empty
        if np.count_nonzero(hdu.data) == 0:
            return None

        # Save a copy in cache and return
        buf = io.BytesIO()
        hdu.writeto(buf)
        buf.seek(0)
        cache[hash_name] = buf.read()

        return fits.open(cache[hash_name])[0]

    return get_hdu(url)


@warningfilter(action="ignore", category=FITSFixedWarning)
def get_finding_chart(
    source_ra,
    source_dec,
    source_name,
    image_source="ps1",
    output_format="pdf",
    imsize=3.0,
    tick_offset=0.02,
    tick_length=0.03,
    fallback_image_source="ps1_cds",
    zscale_contrast=0.045,
    zscale_krej=2.5,
    extra_display_string="",
    **offset_star_kwargs,
):
    """Create a finder chart suitable for spectroscopic observations of
       the source

    Parameters
    ----------
    source_ra : float
        Right ascension (J2000) of the source
    source_dec : float
        Declination (J2000) of the source
    source_name : str
        Name of the source
    image_source : {'desi', 'dss', 'ztfref', 'ps1'}, optional
        Survey where the image comes from "desi", "dss", "ztfref", "ps1"
        defaults to "ps1"
    output_format : str, optional
        "pdf" of "png" -- determines the format of the returned finder
    imsize : float, optional
        Requested image size (on a size) in arcmin. Should be between 2-15.
    tick_offset : float, optional
        How far off the each source should the tick mark be made? (in arcsec)
    tick_length : float, optional
        How long should the tick mark be made? (in arcsec)
    fallback_image_source : str, optional
        Where what `image_source` should we fall back to if the
        one requested fails
    zscale_contrast : float, optional
        Contrast parameter for the ZScale interval
    zscale_krej : float, optional
        Krej parameter for the Zscale interval
    extra_display_string :  str, optional
        What else to show for the source itself in the chart (e.g. proper motion)
    **offset_star_kwargs : dict, optional
        Other parameters passed to `get_nearby_offset_stars`

    Returns
    -------
    dict
        success : bool
            Whether the request was successful or not, returning
            a sensible error in 'reason'
        name : str
            suggested filename based on `source_name` and `output_format`
        data : str
            binary encoded data for the image (to be streamed)
        reason : str
            If not successful, a reason is returned.
    """
    if (imsize < 2.0) or (imsize > 15):
        return {
            "success": False,
            "reason": "Requested `imsize` out of range",
            "data": "",
            "name": "",
        }

    if image_source not in source_image_parameters:
        return {
            "success": False,
            "reason": f"image source {image_source} not in list",
            "data": "",
            "name": "",
        }

    matplotlib.use("Agg")
    fig = plt.figure(figsize=(11, 8.5), constrained_layout=False)
    widths = [2.6, 1]
    heights = [2.6, 1]
    spec = fig.add_gridspec(
        ncols=2,
        nrows=2,
        width_ratios=widths,
        height_ratios=heights,
        left=0.05,
        right=0.95,
    )

    # how wide on the side will the image be? 256 as default
    npixels = source_image_parameters[image_source].get("npixels", 256)
    # set the pixelscale in arcsec (typically about 1 arcsec/pixel)
    pixscale = 60 * imsize / npixels

    hdu = fits_image(source_ra, source_dec, imsize=imsize, image_source=image_source)

    # skeleton WCS - this is the field that the user requested
    wcs = WCS(naxis=2)

    # set the headers of the WCS.
    # The center of the image is the reference point (source_ra, source_dec):
    wcs.wcs.crpix = [npixels / 2, npixels / 2]
    wcs.wcs.crval = [source_ra, source_dec]

    # create the pixel scale and orientation North up, East left
    # pixelscale is in degrees, established in the tangent plane
    # to the reference point
    wcs.wcs.cd = np.array([[-pixscale / 3600, 0], [0, pixscale / 3600]])
    wcs.wcs.ctype = ["RA---TAN", "DEC--TAN"]

    fallback = True
    if hdu is not None:
        im = hdu.data

        # replace the nans with medians
        im[np.isnan(im)] = np.nanmedian(im)

        # Fix the header keyword for the input system, if needed
        hdr = hdu.header
        if "RADECSYS" in hdr:
            hdr.set("RADESYSa", hdr["RADECSYS"], before="RADECSYS")
            del hdr["RADECSYS"]

        if source_image_parameters[image_source].get("reproject", False):
            # project image to the skeleton WCS solution
            log("Reprojecting image to requested position and orientation")
            im, _ = reproject_adaptive(hdu, wcs, shape_out=(npixels, npixels))
        else:
            wcs = WCS(hdu.header)

        if source_image_parameters[image_source].get("smooth", False):
            im = gaussian_filter(
                hdu.data, source_image_parameters[image_source]["smooth"] / pixscale
            )

        cent = int(npixels / 2)
        width = int(0.05 * npixels)
        test_slice = slice(cent - width, cent + width)
        all_nans = np.isnan(im[test_slice, test_slice].flatten()).all()
        all_zeros = (im[test_slice, test_slice].flatten() == 0).all()
        if not (all_zeros or all_nans):
            percents = np.nanpercentile(im.flatten(), [10, 99.0])
            vmin = percents[0]
            vmax = percents[1]
            interval = ZScaleInterval(
                nsamples=int(0.1 * (im.shape[0] * im.shape[1])),
                contrast=zscale_contrast,
                krej=zscale_krej,
            )
            norm = ImageNormalize(im, vmin=vmin, vmax=vmax, interval=interval)
            watermark = source_image_parameters[image_source]["str"]
            fallback = False

    if hdu is None or fallback:
        # if we got back a blank image, try to fallback on another survey
        # and return the results from that call
        if fallback_image_source is not None:
            if fallback_image_source != image_source:
                log(f"Falling back on image source {fallback_image_source}")
                return get_finding_chart(
                    source_ra,
                    source_dec,
                    source_name,
                    image_source=fallback_image_source,
                    output_format=output_format,
                    imsize=imsize,
                    tick_offset=tick_offset,
                    tick_length=tick_length,
                    fallback_image_source=None,
                    **offset_star_kwargs,
                )

        # we dont have an image here, so let's create a dummy one
        # so we can still plot
        im = np.zeros((npixels, npixels))
        watermark = None
        vmin = 0
        vmax = 0
        norm = ImageNormalize(im, vmin=vmin, vmax=vmax)

    # add the images in the top left corner
    ax = fig.add_subplot(spec[0, 0], projection=wcs)
    ax_text = fig.add_subplot(spec[0, 1])
    ax_text.axis("off")
    ax_starlist = fig.add_subplot(spec[1, 0:])
    ax_starlist.axis("off")

    ax.imshow(im, origin="lower", norm=norm, cmap="gray_r")
    ax.set_autoscale_on(False)
    ax.grid(color="white", ls="dotted")
    ax.set_xlabel(r"$\alpha$ (J2000)", fontsize="large")
    ax.set_ylabel(r"$\delta$ (J2000)", fontsize="large")
    obstime = offset_star_kwargs.get("obstime", datetime.datetime.utcnow().isoformat())
    ax.set_title(
        f"{source_name} Finder (for {obstime.split('T')[0]})",
        fontsize="large",
        fontweight="bold",
    )

    star_list, _, _, _, used_ztfref = get_nearby_offset_stars(
        source_ra, source_dec, source_name, **offset_star_kwargs
    )

    if not isinstance(star_list, list) or len(star_list) == 0:
        return {
            "success": False,
            "reason": "failure to get star list",
            "data": "",
            "name": "",
        }

    first_line = None
    if offset_star_kwargs.get("starlist_type", "Keck") == "P200-NGPS":
        # add a first line with the column names for P200-NGPS (csv format)
        first_line = "NAME,RA,DECL,OFFSET_RA,OFFSET_DEC,COMMENT,PRIORITY,BINSPAT,BINSPECT,SLITANGLE,SLITWIDTH,AIRMASS_MAX,WRANGE_LOW,WRANGE_HIGH,CHANNEL,MAGNITUDE,MAGFILTER,EXPTIME"

    ncolors = len(star_list)
    if star_list[0]["str"].startswith("!Data"):
        ncolors -= 1
    colors = sns.color_palette("colorblind", ncolors)

    start_text = [-0.45, 0.99]
    origin = "GaiaDR3" if not used_ztfref else "ZTFref"
    starlist_url = urllib.parse.urljoin(
        HOST,
        f"/api/sources/{source_name}/offsets?"
        f"facility={offset_star_kwargs.get('starlist_type', 'Keck')}",
    )
    starlist_str = (
        f"# Note: {origin} used for offset star positions\n"
        "# Note: spacing in starlist many not copy/paste correctly in PDF\n"
        + "#       you can get starlist directly from"
        + f" {starlist_url}\n"
        + (f"{first_line}\n" if first_line else "")
        + "\n".join([x["str"] for x in star_list])
    )

    # add the starlist
    ax_starlist.text(
        0,
        0.50,
        starlist_str,
        fontsize="x-small",
        family="monospace",
        transform=ax_starlist.transAxes,
    )

    # add the watermark for the survey
    props = {"boxstyle": "round", "facecolor": "gray", "alpha": 0.7}

    if watermark is not None:
        ax.text(
            0.035,
            0.035,
            watermark,
            horizontalalignment="left",
            verticalalignment="center",
            transform=ax.transAxes,
            fontsize="medium",
            fontweight="bold",
            color="yellow",
            alpha=0.5,
            bbox=props,
        )

    date_obs = hdr.get("DATE-OBS")
    if not date_obs:
        mjd_obs = hdr.get("MJD-OBS")
        if mjd_obs:
            date_obs = Time(f"{mjd_obs}", format="mjd").to_value(
                "fits", subfmt="date_hms"
            )

    if date_obs:
        ax.text(
            0.95,
            0.95,
            f"image date {date_obs.split('T')[0]}",
            horizontalalignment="right",
            verticalalignment="center",
            transform=ax.transAxes,
            fontsize="small",
            color="yellow",
            alpha=0.5,
            bbox=props,
        )

    ax.text(
        0.95,
        0.035,
        f"{imsize}\u2032 \u00d7 {imsize}\u2032",  # size'x size'
        horizontalalignment="right",
        verticalalignment="center",
        transform=ax.transAxes,
        fontsize="medium",
        fontweight="bold",
        color="yellow",
        alpha=0.5,
        bbox=props,
    )

    # compass rose
    # rose_center_pixel = ax.transAxes.transform((0.04, 0.95))
    rose_center = pixel_to_skycoord(int(npixels * 0.1), int(npixels * 0.9), wcs)
    props = {"boxstyle": "round", "facecolor": "gray", "alpha": 0.5}

    for ang, label, off in [(0, "N", 0.01), (90, "E", 0.03)]:
        position_angle = ang * u.deg
        separation = (0.05 * imsize * 60) * u.arcsec  # 5%
        p2 = rose_center.directional_offset_by(position_angle, separation)
        ax.plot(
            [rose_center.ra.value, p2.ra.value],
            [rose_center.dec.value, p2.dec.value],
            transform=ax.get_transform("world"),
            color="gold",
            linewidth=2,
        )

        # label N and E
        position_angle = (ang + 15) * u.deg
        separation = ((0.05 + off) * imsize * 60) * u.arcsec
        p2 = rose_center.directional_offset_by(position_angle, separation)
        ax.text(
            p2.ra.value,
            p2.dec.value,
            label,
            color="gold",
            transform=ax.get_transform("world"),
            fontsize="large",
            fontweight="bold",
        )

    # account for Shane header
    if star_list[0]["str"].startswith("!Data"):
        star_list = star_list[1:]

    for i, star in enumerate(star_list):
        c1 = SkyCoord(star["ra"] * u.deg, star["dec"] * u.deg, frame="icrs")

        # mark up the right side of the page with position and offset info
        name_title = star["name"]
        if star.get("mag") is not None:
            name_title += f" {star.get('mag'):.2f} mag"
        ax_text.text(
            start_text[0],
            start_text[1] - (i * 1.1) / ncolors,
            name_title,
            ha="left",
            va="top",
            fontsize="large",
            fontweight="bold",
            transform=ax_text.transAxes,
            color=colors[i],
        )
        source_text = f"  {star['ra']:.5f} {star['dec']:.5f}\n"
        source_text += f"  {c1.to_string('hmsdms', precision=2)}\n"
        if i == 0 and extra_display_string != "":
            source_text += f"  {extra_display_string}\n"
        if (
            (star.get("dras") is not None)
            and (star.get("ddecs") is not None)
            and (star.get("pa") is not None)
        ):
            source_text += f"  {star.get('dras')} {star.get('ddecs')} (PA={star.get('pa'):<0.02f}°)"
        ax_text.text(
            start_text[0],
            start_text[1] - (i * 1.1) / ncolors - 0.06,
            source_text,
            ha="left",
            va="top",
            fontsize="large",
            transform=ax_text.transAxes,
            color=colors[i],
        )

        # work on making marks where the stars are
        for ang in [0, 90]:
            # for the source itself (i=0), change the angle of the lines in
            # case the offset star is the same as the source itself
            position_angle = ang * u.deg if i != 0 else (ang + 225) * u.deg
            separation = (tick_offset * imsize * 60) * u.arcsec
            p1 = c1.directional_offset_by(position_angle, separation)
            separation = (tick_offset + tick_length) * imsize * 60 * u.arcsec
            p2 = c1.directional_offset_by(position_angle, separation)
            ax.plot(
                [p1.ra.value, p2.ra.value],
                [p1.dec.value, p2.dec.value],
                transform=ax.get_transform("world"),
                color=colors[i],
                linewidth=3 if imsize <= 4 else 2,
                alpha=0.8,
            )
        if star["name"].find("_o") != -1:
            # this is an offset star
            text = star["name"].split("_o")[-1]
            position_angle = 14 * u.deg
            separation = (tick_offset + tick_length * 1.6) * imsize * 60 * u.arcsec
            p1 = c1.directional_offset_by(position_angle, separation)
            ax.text(
                p1.ra.value,
                p1.dec.value,
                text,
                color=colors[i],
                transform=ax.get_transform("world"),
                fontsize="large",
                fontweight="bold",
            )

    buf = io.BytesIO()
    fig.savefig(buf, format=output_format)
    plt.close(fig)
    buf.seek(0)

    return {
        "success": True,
        "name": f"finder_{source_name}.{output_format}",
        "data": buf.read(),
        "reason": "",
    }

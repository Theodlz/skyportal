import numpy as np
import sncosmo
import astropy.units as u
from sncosmo.bandpasses import _BANDPASSES
from matplotlib import cm
from matplotlib.colors import LinearSegmentedColormap, rgb2hex
from baselayer.app.env import load_env
from baselayer.log import make_log

_, cfg = load_env()


log = make_log("api/photometry")

cmap_ir = cm.get_cmap("autumn")
cmap_deep_ir = LinearSegmentedColormap.from_list(
    "deep_ir", [(0.8, 0.2, 0), (0.6, 0.1, 0)]
)

# load additional bandpasses into the SN comso registry
existing_bandpasses_names = [val["name"] for val in _BANDPASSES.get_loaders_metadata()]
additional_bandpasses_names = []
for additional_bandpasses in cfg.get("additional_bandpasses", []):
    name = additional_bandpasses.get("name")
    if not name:
        continue
    if name in existing_bandpasses_names:
        log(
            f"Additional Bandpass name={name} is already in the sncosmo registry. Skipping."
        )
    try:
        wavelength = np.array(additional_bandpasses.get("wavelength"))
        transmission = np.array(additional_bandpasses.get("transmission"))
        band = sncosmo.Bandpass(wavelength, transmission, name=name, wave_unit=u.AA)
    except Exception as e:
        log(f"Could not make bandpass for {name}: {e}")
        continue

    sncosmo.registry.register(band)
    additional_bandpasses_names.append(name)

if len(additional_bandpasses_names) > 0:
    log(f"registered custom bandpasses: {additional_bandpasses_names}")

ALLOWED_BANDPASSES = tuple(existing_bandpasses_names + additional_bandpasses_names)

def hex2rgb(hex):
    """Convert hex color string to rgb tuple.

    Parameters
    ----------
    hex : str
        Hex color string.

    Returns
    -------
    tuple
        RGB tuple.
    """

    return tuple(int(hex[i : i + 2], 16) for i in (0, 2, 4))


def get_effective_wavelength(bandpass_name, radius=None):
    """Get the effective wavelength of an sncosmo bandpass.

    Parameters
    ----------
    bandpass_name : str
        Name of the bandpass.
    radius : float, optional
        Radius to get the bandpass for. If None, the default bandpass is used.

    Returns
    -------
    float
        Effective wavelength of the bandpass.
    """
    try:
        args = {}
        if radius is not None:
            args["radius"] = radius
        bandpass = sncosmo.get_bandpass(bandpass_name, **args)
    except ValueError as e:
        raise ValueError(
            f"Could not get bandpass for {bandpass_name} due to sncosmo error: {e}"
        )

    return float(bandpass.wave_eff)


def get_color(bandpass, format="hex"):
    """Get a color for a bandpass, in hex or rgb format.

    Parameters
    ----------
    bandpass : str
        Name of the sncosmo bandpass.
    format : str, optional
        Format of the output color. Must be one of "hex" or "rgb".

    Returns
    -------
    str or tuple
        Color of the bandpass in the requested format
    """

    wavelength = get_effective_wavelength(bandpass)

    if 0 < wavelength <= 1500:  # EUV
        bandcolor = "#4B0082"
    elif 1500 < wavelength <= 2100:  # uvw2
        bandcolor = "#6A5ACD"
    elif 2100 < wavelength <= 2400:  # uvm2
        bandcolor = "#9400D3"
    elif 2400 < wavelength <= 3000:  # uvw1
        bandcolor = "#FF00FF"
    elif 3000 < wavelength <= 4000:  # U, sdss u
        bandcolor = "#0000FF"
    elif 4000 < wavelength <= 4800:  # B, sdss g
        bandcolor = "#02d193"
    elif 4800 < wavelength <= 5000:  # ztfg
        bandcolor = "#008000"
    elif 5000 < wavelength <= 6000:  # V
        bandcolor = "#9ACD32"
    elif 6000 < wavelength <= 6400:  # sdssr
        bandcolor = "#ff6f00"
    elif 6400 < wavelength <= 6600:  # ztfr
        bandcolor = "#FF0000"
    elif 6400 < wavelength <= 7000:  # bessellr, atlaso
        bandcolor = "#c80000"
    elif 7000 < wavelength <= 8000:  # sdss i
        bandcolor = "#FFA500"
    elif 8000 < wavelength <= 9000:  # sdss z
        bandcolor = "#A52A2A"
    elif 9000 < wavelength <= 10000:  # PS1 y
        bandcolor = "#B8860B"
    elif 10000 < wavelength <= 13000:  # 2MASS J
        bandcolor = "#000000"
    elif 13000 < wavelength <= 17000:  # 2MASS H
        bandcolor = "#9370D8"
    elif 17000 < wavelength <= 1e5:  # mm to Radio
        bandcolor = rgb2hex(cmap_ir((5 - np.log10(wavelength)) / 0.77)[:3])
    elif 1e5 < wavelength <= 3e5:  # JWST miri and miri-tophat
        bandcolor = rgb2hex(cmap_deep_ir((5.48 - np.log10(wavelength)) / 0.48)[:3])
    else:
        log(
            f"{bandpass} with effective wavelength {wavelength} is out of range for color maps, using black"
        )
        bandcolor = "#000000"

    if format == "rgb":
        return hex2rgb(bandcolor[1:])
    elif format not in ["hex", "rgb"]:
        raise ValueError(f"Invalid color format: {format}")

    return bandcolor


def get_bandpasses_to_colors(bandpasses, colors_type="rgb"):
    return {bandpass: get_color(bandpass, colors_type) for bandpass in bandpasses}


def get_filters_mapper(photometry):
    filters = {phot["filter"] for phot in photometry}
    return get_bandpasses_to_colors(filters)

def get_treasuremap_filters():
    # this is the list of filters that are available in the treasuremap
    treasuremap_filters = {
        "U": "U",
        "B": "B",
        "V": "V",
        "R": "R",
        "I": "I",
        "J": "J",
        "H": "H",
        "K": "K",
        "u": "u",
        "g": "g",
        "r": "r",
        "i": "i",
        "z": "z",
        "UVW1": "UVW1",
        "UVM2": "UVM2",
        "XRT": "XRT",
        "clear": "clear",
        "open": "open",
        "UHF": "UHF",
        "VHF": "VHF",
        "L": "L",
        "S": "S",
        "C": "C",
        "X": "X",
        "other": "other",
        "TESS": "TESS",
        "BAT": "BAT",
        "HESS": "HESS",
        "WISEL": "WISEL",
    }
    # to it, we add mappers for sncosmo bandpasses
    for bandpass_name in ALLOWED_BANDPASSES:
        try:
            bandpass = sncosmo.get_bandpass(bandpass_name)
            central_wavelength = (bandpass.minwave() + bandpass.maxwave()) / 2
            bandwidth = bandpass.maxwave() - bandpass.minwave()
            treasuremap_filters[bandpass_name] = [central_wavelength, bandwidth]
        except Exception as e:
            log(f"Error adding bandpass {bandpass_name} to treasuremap filters: {e}")

    # overwrite the filters for ZTF, as i-band is will otherwise be matched to TESS by treasuremap
    treasuremap_filters["ztfg"] = "g"
    treasuremap_filters["ztfr"] = "r"
    treasuremap_filters["ztfi"] = "i"

    return treasuremap_filters


BANDPASSES_COLORS = get_bandpasses_to_colors(ALLOWED_BANDPASSES, "rgb")
BANDPASSES_WAVELENGTHS = {
    bandpass: get_effective_wavelength(bandpass) for bandpass in ALLOWED_BANDPASSES
}
TREASUREMAP_FILTERS = get_treasuremap_filters()
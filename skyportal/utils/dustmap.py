import os
import time
import dustmaps.sfd
import requests
from dustmaps.config import config

from baselayer.app.env import load_env
from baselayer.log import make_log

_, cfg = load_env()
log = make_log("dustmap")

# download dustmap if required
# we want to avoid multiple processes trying to download the dustmaps concurrently
# so, once one procress stars downloading the dustmaps, it creates a lock file
# other processes will then see the lock file and skip trying to download the dustmaps
config["data_dir"] = cfg["misc.dustmap_folder"]
required_files = ["sfd/SFD_dust_4096_ngp.fits", "sfd/SFD_dust_4096_sgp.fits"]
lockfile = os.path.join(cfg["misc.dustmap_folder"], "download_in_progress.lock")
if (
    any(
        not os.path.isfile(os.path.join(config["data_dir"], required_file))
        for required_file in required_files
    )
):
    if not os.path.isfile(lockfile):
        # if no process is currently downloading the dustmaps, start downloading
        try:
            log("Downloading dustmaps...")
            os.makedirs(cfg["misc.dustmap_folder"], exist_ok=True)
            open(lockfile, "w").close()
            dustmaps.sfd.fetch()
        except (requests.exceptions.HTTPError, OSError) as e:
            log(f"Error downloading dustmaps: {e}")
            pass
        finally:
            os.remove(lockfile)
    else:
        # wait until the process currently downloading the dustmaps is done
        while os.path.isfile(lockfile):
            log("Waiting for dustmaps to finish downloading (another process is downloading them)...")
            time.sleep(1)
            pass

SFDQuery: dustmaps.sfd.SFDQuery = dustmaps.sfd.SFDQuery()
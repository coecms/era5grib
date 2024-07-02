import xarray as xr
from ..config import conf
import subprocess
import tempfile
import os

def write(ds: xr.Dataset):

    ds.time.encoding["units"] = "hours since 1970-01-01"

    encoding = {
        k: {"complevel": 0, "chunksizes": None, "_FillValue": -1e10}
        for k in ds.keys()
    }

    ### Make a temp netcdf file
    with tempfile.NamedTemporaryFile(dir=os.environ.get('TMPDIR','/tmp')) as f:
        tmp_name = f.name
        ds.to_netcdf(tmp_name,encoding=encoding)
        subprocess.run(["cdo", "-v", "-f", "grb1", "-t", "ecmwf", "copy", tmp_name, conf.get("output")],check=True)

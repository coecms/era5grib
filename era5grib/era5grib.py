#!/g/data/hh5/public/apps/nci_scripts/python-analysis3
# Copyright 2020 Scott Wales
# author: Scott Wales <scott.wales@unimelb.edu.au>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import xarray
import tempfile
import subprocess
import time
from . import nci as nci
#from . import clex as clex
import logging
from pathlib import Path

from . import main


def save_grib(ds, output, format="grib",  era5land=True):
    """
    Save a dataset to GRIB format
    """

    print("Saving...")
    # should be this changed as now data goes back to 1950?
    ds.time.encoding["units"] = "hours since 1970-01-01"

    if format == "netcdf":
        #climtas.io.to_netcdf_throttled(ds, output)
        ds.to_netcdf(output)
        return

    with tempfile.NamedTemporaryFile(delete=False) as tmp1, tempfile.NamedTemporaryFile(delete=False) as tmp2:
        logging.info("Creating intermediate file")
        # issue #11  skipping call to netcdf_throttled as it fails when using
        # era5land data
        # this is a temporary fix to make sure files get saved
        # it could potentially fail for bigger files
        if not era5land:
            # Saving with compression is fast here
            tmp_compressed = tmp1.name
            #climtas.io.to_netcdf_throttled(ds, tmp_compressed)
            logging.info("Decompressing intermediate file")
            mark = time.perf_counter()
            # Decompress the data for CDO's benefit
            ds = xarray.open_dataset(tmp_compressed, chunks={"time": 1})
        # resume from here issue #11 temp fix
        print("Whats happening...")
        encoding = {
            k: {"complevel": 0, "chunksizes": None, "_FillValue": -1e10}
            for k in ds.keys()
        }
        tmp_uncompressed = tmp2.name
        mark = time.perf_counter()
        ds.to_netcdf(tmp_uncompressed, encoding=encoding)
        logging.info(f"Decompress time {time.perf_counter() - mark}")

        logging.info("Converting to GRIB")
        # CDO is faster with uncompressed data
        subprocess.run(
            ["cdo", "-v", "-f", "grb1", "-t", "ecmwf", "copy", tmp_uncompressed, output],
            check=True,
        )


def select_domain(ds, lats, lons):
    error = False
    message = "ERROR: Target area is outside the ERA5 archive domain"

    logging.info(f"Latitudes: Target ({lats.min():.2f}:{lats.max():.2f})")
    logging.info(f"Longitudes: Target ({lons.min():.2f}:{lons.max():.2f})")

    if ds.longitude[-1] < 180 and lons.max() > 180:
        # Roll longitude
        ds = ds.roll(longitude=ds.sizes["longitude"] // 2, roll_coords=True)
        ds = ds.assign_coords(longitude=(ds.longitude + 360) % 360)

    if lats.max() > ds.latitude[0] or lats.min() < ds.latitude[-1]:
        error = True
        message += f"\n    Latitudes: Target ({lats.min():.2f}:{lats.max():.2f}), ERA5 ({ds.latitude.values[0]}:{ds.latitude.values[-1]})"
    if lons.min() < ds.longitude[0] or lons.max() > ds.longitude[-1]:
        error = True
        message += f"\n    Longitudes: Target ({lons.min():.2f}:{lons.max():.2f}), ERA5 ({ds.longitude.values[0]}:{ds.longitude.values[-1]})"
        message += "\nTry the --polar flag to include all longitudes"

    if error:
        raise IndexError(message)

    return ds.sel(
        latitude=slice(lats.max() + 1, lats.min() - 1),
        longitude=slice(lons.min() - 1, lons.max() + 1),
    )


def era5grib_wrf(
    start=None,
    end=None,
    output=None,
    namelist=None,
    geo=None,
    source="NCI",
    format="grib",
    era5land: bool = True,
    polar: bool = False,
):
    """
    Legacy entry point preserved for compatibility

    Convert the NCI ERA5 archive data to GRIB format for use in WRF limited
    area modelling.

    Will generate a grib file for a time range (including the fixed z and lsm
    fields), to be processed by WPS's ungrib tool. Dates can be read from a WPS
    namelist or by supplying a start and end time.

    The output area can be limited to reduce file size by supplying the geogrid
    output file as 'geo'.
    """
    if era5land:
        conf_file = Path(__file__).parent.parent / 'config' / 'wrf_era5land.yaml'
    else:
        conf_file = Path(__file__).parent.parent / 'config' / 'wrf_era5.yaml'

    if polar:
        polar_flag="--polar"
    else:
        polar_flag="--no-polar"

    main(["--file",conf_file,"--start",start,"--end",end,"--output",output,"--namelist",namelist,"--geo",geo,"--format",format,polar_flag])


def era5grib_um(
    time,
    output=None,
    target=None,
    source="NCI",
    format="grib",
    era5land: bool = True,
    polar: bool = False,
):
    """
    Legacy entry point preserved for compatibility

    Convert the NCI ERA5 archive data to GRIB format for use in UM limited area
    modelling.

    Will generate a grib file for a single time, to be processed by the UM
    reconfiguration.

    The output area can be limited to reduce file size by supplying a UM file
    on the target grid as 'target'
    """

    if era5land:
        conf_file = Path(__file__).parent.parent / 'config' / 'um_era5land.yaml'
    else:
        conf_file = Path(__file__).parent.parent / 'config' / 'um_era5.yaml'

    if polar:
        polar_flag="--polar"
    else:
        polar_flag="--no-polar"

    main(["--file",conf_file,"--time",time,"--output",output,"--target",target,"--format",format,polar_flag])

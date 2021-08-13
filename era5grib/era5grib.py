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

import argparse
import pandas
import xarray
import numpy
import os
import tempfile
import subprocess
from climtas.regrid import Regridder
import climtas
import climtas.nci
import mule
from glob import glob
import f90nml
from pkg_resources import resource_stream, resource_filename
import textwrap
from tqdm import tqdm
import time
import dask.diagnostics
from . import nci as nci
from . import clex as clex
import logging


def save_grib(ds, output, format="grib"):
    """
    Save a dataset to GRIB format
    """

    ds.time.encoding["units"] = "hours since 1970-01-01"

    if format == "netcdf":
        climtas.io.to_netcdf_throttled(ds, output)
        return

    with tempfile.NamedTemporaryFile() as tmp1, tempfile.NamedTemporaryFile() as tmp2:
        logging.info("Creating intermediate file")
        # Saving with compression is fast here
        tmp_compressed = tmp1.name
        climtas.io.to_netcdf_throttled(ds, tmp_compressed)

        logging.info("Decompressing intermediate file")
        mark = time.perf_counter()
        # Decompress the data for CDO's benefit
        ds = xarray.open_dataset(tmp_compressed, chunks={"time": 1})
        encoding = {
            k: {"complevel": 0, "chunksizes": None, "_FillValue": -1e10}
            for k in ds.keys()
        }
        tmp_uncompressed = tmp2.name
        ds.to_netcdf(tmp_uncompressed, encoding=encoding)
        logging.info(f"Decompress time {time.perf_counter() - mark}")

        logging.info("Converting to GRIB")
        # CDO is faster with uncompressed data
        subprocess.run(
            ["cdo", "-f", "grb1", "-t", "ecmwf", "copy", tmp_uncompressed, output],
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
):
    """
    Convert the NCI ERA5 archive data to GRIB format for use in WRF limited
    area modelling.

    Will generate a grib file for a time range (including the fixed z and lsm
    fields), to be processed by WPS's ungrib tool. Dates can be read from a WPS
    namelist or by supplying a start and end time.

    The output area can be limited to reduce file size by supplying the geogrid
    output file as 'geo'.
    """
    if namelist is not None:
        with open(namelist, "r") as f:
            nml = f90nml.read(f)
            if start is None:
                start = pandas.to_datetime(
                    nml["share"]["start_date"], format="%Y-%m-%d_%H:%M:%S"
                )
                try:
                    start = start.min()
                except:
                    pass
            if end is None:
                end = pandas.to_datetime(
                    nml["share"]["end_date"], format="%Y-%m-%d_%H:%M:%S"
                )
                try:
                    end = end.max()
                except:
                    pass

    if start is None:
        raise Exception("Please provide either 'start' or 'namelist'")

    if end is None:
        end = start

    if output is None:
        output = "GRIBFILE.AAA"

    logging.info(f"Time: Target ({start}:{end})")

    if source == "CLEX":
        ds = clex.read_wrf(start, end)
    else:
        ds = nci.read_wrf(start, end, era5land=era5land)

    if geo is not None:
        geo = xarray.open_dataset(geo)

        lons = geo.XLONG_M.where(geo.XLONG_M > 0, geo.XLONG_M + 360)

        ds = select_domain(ds, lats=geo.XLAT_M.values, lons=lons.values)

    else:
        logging.warn("Outputting the full domain, use --geo=geo_em.d01.nc to limit")

    save_grib(ds, output, format=format)

    logging.info(f"Wrote {output}")


def era5grib_um(
    time, output=None, target=None, source="NCI", format="grib", era5land: bool = True
):
    """
    Convert the NCI ERA5 archive data to GRIB format for use in UM limited area
    modelling.

    Will generate a grib file for a single time, to be processed by the UM
    reconfiguration.

    The output area can be limited to reduce file size by supplying a UM file
    on the target grid as 'target'
    """

    logging.info(f"Time: Target ({time})")

    if source == "CLEX":
        ds = clex.read_um(time)
    else:
        ds = nci.read_um(time, era5land=era5land)

    if output is None:
        output = f"um.era5.{pandas.to_datetime(ds.time.values[0]).strftime('%Y%m%dT%H%M')}.grib"

    if target is not None:
        mf = mule.load_umfile(target)

        ny = mf.integer_constants.num_rows
        nx = mf.integer_constants.num_cols

        y0 = mf.real_constants.start_lat
        x0 = mf.real_constants.start_lon

        dy = mf.real_constants.row_spacing
        dx = mf.real_constants.col_spacing

        lat = y0 + numpy.arange(ny) * dy
        lon = x0 + numpy.arange(nx) * dx

        print(x0, dx, nx, lon[0], lon[-1])
        print(y0, dy, ny, lat[0], lat[-1])

        ds = select_domain(ds, lats=lat, lons=lon)

    else:
        logging.warn("Outputting the full domain, use --target=qrparm.mask to limit")

    save_grib(ds, output, format=format)

    logging.info(f"Wrote {output}")


def main():
    """
    Convert the NCI ERA5 archive data to GRIB format for use in limited area modelling.

    Can output the required fields for both WRF and UM models
    """
    f = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        description=textwrap.dedent(main.__doc__), formatter_class=f
    )
    subp = parser.add_subparsers()

    wrf = subp.add_parser(
        "wrf",
        help="Output fields for WRF",
        description=textwrap.dedent(era5grib_wrf.__doc__),
        formatter_class=f,
    )
    wrf.set_defaults(func=era5grib_wrf)
    wrf.add_argument("--namelist", help="Read start and end dates from WPS namelist")
    wrf.add_argument("--start", help="Output start time", type=pandas.to_datetime)
    wrf.add_argument("--end", help="Output end time", type=pandas.to_datetime)
    wrf.add_argument("--output", help="Output file")
    wrf.add_argument("--geo", help="Geogrid file for trimming (e.g. geo_em.d01.nc)")
    wrf.add_argument(
        "--format", help="Output format", choices=["grib", "netcdf"], default="grib"
    )
    wrf.add_argument(
        "--source", help="Data project source", choices=["NCI", "CLEX"], default="NCI"
    )
    wrf.add_argument(
        "--era5land",
        help="Use era5land over land",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    wrf.add_argument("--debug", help="Debug output", action="store_true")

    um = subp.add_parser(
        "um",
        help="Output fields for UM",
        description=textwrap.dedent(era5grib_um.__doc__),
        formatter_class=f,
    )
    um.set_defaults(func=era5grib_um)
    um.add_argument("--time", help="Output time", required=True)
    um.add_argument("--output", help="Output file")
    um.add_argument(
        "--target", help="UM file on the target grid for trimming (e.g. qrparm.mask)"
    )
    um.add_argument(
        "--format", help="Output format", choices=["grib", "netcdf"], default="grib"
    )
    um.add_argument(
        "--source", help="Data project source", choices=["NCI", "CLEX"], default="NCI"
    )
    um.add_argument(
        "--era5land",
        help="Use era5land over land",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    um.add_argument("--debug", help="Debug output", action="store_true")

    args = parser.parse_args()

    if os.environ["HOSTNAME"].startswith("gadi-login"):
        tmpdir = tempfile.TemporaryDirectory()
        client = dask.distributed.Client(
            n_workers=2,
            threads_per_worker=1,
            memory_limit="1gb",
            local_directory=tmpdir.name,
        )
    else:
        client = climtas.nci.GadiClient()

    dargs = vars(args)
    func = dargs.pop("func")
    debug = dargs.pop("debug")

    if debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    func(**dargs)

    client.close(timeout=3)


if __name__ == "__main__":
    main()

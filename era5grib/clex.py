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
import iris
from glob import glob
import f90nml
from pkg_resources import resource_stream, resource_filename
import textwrap
from tqdm import tqdm
import time
import dask.diagnostics
import logging


chunks = {
    "surface": {"time": 93, "latitude": 91, "longitude": 180},
    "land": {"time": 54, "latitude": 129, "longitude": 258},
    "pressure": {"time": 93, "latitude": 39, "longitude": 72, "level": -1},
}

fx = None
catalogue = None
regridder = None  # Regridder(weights=weights)


def init():
    global fx

    fx = xarray.open_dataset("/g/data/ub4/era5/netcdf/static_era5.nc").isel(time=0)
    fx = fx.sel(latitude=slice(20, -57), longitude=slice(78, 220))
    fx.lsm.attrs["code"] = numpy.int32(172)
    fx.z.attrs["code"] = numpy.int32(129)

    global catalogue
    catalogue = pandas.read_csv(
        resource_stream(__name__, "catalogue.csv"), index_col=["product", "varname"]
    )
    weights = xarray.open_dataset(resource_filename(__name__, "clex_regrid_weights.nc"))

    global regridder
    regridder = Regridder(weights=weights)


def land_subset(da):

    da = da.roll(longitude=3600 // 2, roll_coords=True)

    attrs = da["longitude"].attrs
    da["longitude"] = numpy.concatenate(
        [da.longitude[: 3600 // 2], da.longitude[3600 // 2 :] + 360]
    )
    da["longitude"].attrs = attrs

    da = da.sel(latitude=slice(19.5, -57.5), longitude=slice(77.5, 220.5))
    return da


def gen_weights():
    source = xarray.open_dataset(
        "/g/data/ub4/era5/netcdf/land/skt/2020/skt_era5land_global_20200101_20200131.nc",
        chunks=chunks["land"],
    )["skt"]
    source = land_subset(source.isel(time=0))
    target = fx.lsm.where(fx.lsm > 0)

    weights = climtas.regrid.esmf_generate_weights(source, target, method="patch")

    encoding = {}
    for k, v in weights.items():
        encoding[k] = {"zlib": True, "shuffle": True, "complevel": 8}
        if v.dtype == "float64":
            encoding[k]["dtype"] = "float32"

    weights.to_netcdf("regrid_weights.nc", encoding=encoding)


def read_era5_raw(entry, start, end):
    t0 = pandas.offsets.MonthBegin().rollback(start.date())
    t1 = pandas.offsets.MonthEnd().rollforward(end.date())
    paths = []

    product = entry.name[0]
    var = entry.name[1]
    pattern = None

    source = {"land": "era5land", "surface": "era5", "pressure": "era5"}[product]
    domain = {"land": "global", "surface": "global", "pressure": "aus"}[product]

    for ms, me in zip(
        pandas.date_range(t0, t1, freq="MS"), pandas.date_range(t0, t1, freq="M")
    ):
        pattern = os.path.join(
            entry["dirname"],
            ms.strftime("%Y"),
            f'*_{source}_{domain}_{ms.strftime("%Y%m%d")}_{me.strftime("%Y%m%d")}.nc',
        )
        path = glob(pattern)
        paths.extend(path)

    logging.debug(paths)

    try:
        da = xarray.open_mfdataset(paths, chunks=chunks[product], concat_dim="time")[
            var
        ]
    except OSError:
        raise IndexError(
            f"ERROR: No ERA5 data found, check model dates are within the ERA5 period, you are a member of ub4 and that -lstorage includes gdata/ub4 if running in the queue (requesting {product} {var} {t0} {t1}, {pattern})"
        )

    da = da.sel(time=slice(start, end))

    if product == "land":
        # Regrid
        da = land_subset(da)
        da = regridder.regrid(da)

    if product == "surface":
        # Select region
        da = da.roll(longitude=1440 // 2, roll_coords=True)
        da["longitude"] = numpy.concatenate(
            [da.longitude[: 1440 // 2], da.longitude[1440 // 2 :] + 360]
        )
        da = da.sel(latitude=slice(20, -57), longitude=slice(78, 220))

    da.attrs["code"] = numpy.int32(entry["code"])
    da.attrs["table"] = numpy.int32(entry["table"])
    da.attrs["era5_name"] = var
    da.attrs["product"] = product

    da.encoding.setdefault("_FillValue", -1e10)

    return da


def read_surface(var, start, end):
    """
    Read a surface level variable from era5

    Checks both 'surface' and 'era5land'

    If the variable is present in both, 'era5land' values are used over land
    (regridded to the era5 grid), and 'surface' values over the ocean
    """

    # Variables to use from surface to fill in ocean points, if different from var
    surf_vars = {"skt": "sst"}
    surf_var = surf_vars.get(var, var)

    # Grab the variable from surface if its available
    try:
        da_surf = read_era5_raw(catalogue.loc["surface", surf_var], start, end)

    except KeyError:
        da_surf = None

    # Then grab the variable from era5land
    try:
        da = read_era5_raw(catalogue.loc["land", var], start, end)

        # Fill in over oceans if we found the variable in surface
        if da_surf is not None:
            da = da.where(fx.lsm.data > 0, da_surf.data)
            da.attrs["product"] = "mixed era5land / era5 surface"
            da.encoding.setdefault("_FillValue", -1e10)

    except KeyError:
        da = da_surf

    if da is None:
        raise KeyError(var)

    da.attrs["var"] = var
    da.name = f"{var}_surf"

    return da


def read_vertical(var, start, end):
    """
    Read a vertical level variable from era5
    """
    entry = catalogue.loc["pressure", var]
    da = read_era5_raw(entry, start, end)
    da.name = f"{var}_pl"
    return da


def read_era5(surface, vertical, start, end):
    ds = xarray.Dataset()
    ds["lsm"] = fx.lsm
    ds["z"] = fx.z

    logging.info("Selecing ERA5 archive files")
    progress = tqdm(total=len(surface) + len(vertical))

    for var in surface:
        da = read_surface(var, start, end)
        ds[da.name] = da
        progress.update()

    for var in vertical:
        da = read_vertical(var, start, end)
        ds[da.name] = da
        progress.update()

    return ds


def save_grib(ds, output):
    """
    Save a dataset to GRIB format
    """
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


def read_um(time):

    init()

    # Make sure the time includes an hour
    start = pandas.offsets.Hour().rollback(time)
    ds = read_era5(
        [
            "skt",
            "sp",
            # "siconc",
            "sde",
            "stl1",
            "stl2",
            "stl3",
            "stl4",
            "swvl1",
            "swvl2",
            "swvl3",
            "swvl4",
        ],
        ["u", "v", "t", "q"],
        start,
        start,
    )

    for k, v in ds.items():
        ds[k] = v.fillna(v.mean())

    ds = soil_level_metadata(ds)

    return ds


def read_wrf(start, end):

    init()

    ds = read_era5(
        [
            "u10",
            "v10",
            "t2m",
            "d2m",
            "sp",
            "msl",
            "skt",
            # "siconc",
            "sst",
            "sde",
            "stl1",
            "stl2",
            "stl3",
            "stl4",
            "swvl1",
            "swvl2",
            "swvl3",
            "swvl4",
        ],
        ["z", "u", "v", "t", "r"],
        start,
        end,
    )

    ds = soil_level_metadata(ds)

    return ds


def soil_level_metadata(ds):
    depth = [None, 3.5, 17.5, 64, 177.5]
    depth_bnds = [None, 0, 7, 28, 100, 255]

    depth_attrs = {
        "long_name": "depth_below_land",
        "units": "cm",
        "positive": "down",
        "axis": "Z",
    }

    for l in range(1, 5):
        ds[f"stl{l}_surf"] = ds[f"stl{l}_surf"].expand_dims(f"depth{l}", axis=1)
        ds[f"swvl{l}_surf"] = ds[f"swvl{l}_surf"].expand_dims(f"depth{l}", axis=1)
        ds.coords[f"depth{l}"] = xarray.DataArray(
            depth[l : (l + 1)], dims=[f"depth{l}"], attrs=depth_attrs
        )
        ds.coords[f"depth{l}_bnds"] = xarray.DataArray(
            [depth_bnds[l : (l + 2)]], dims=[f"depth{l}", "bnds"], attrs=depth_attrs
        )
        ds.coords[f"depth{l}"].attrs["bounds"] = f"depth{l}_bnds"

    return ds

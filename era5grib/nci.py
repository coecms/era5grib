#!/g/data/hh5/public/apps/nci_scripts/python-analysis3
# Copyright 2021 Scott Wales
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

import intake
import xesmf
import xarray
import functools
import pandas
import numpy
import logging
from pkg_resources import resource_filename


@functools.lru_cache
def paramdb():
    return intake.cat.nci.ecmwf.grib_parameters.read()


def load_var(cat, chunks={"time": 12}, **kwargs):
    """
    Load a single variable from intake

    Args:
        cat: intake-esm catalogue
        **kwargs: catalogue search terms

    Returns:
        xarray.DataArray with a single variable
    """
    result = cat.search(**kwargs)

    if len(result.df) == 0:
        raise Exception(f"No matches: {cat.name} {kwargs}")

    # Prefer era5-1
    if "era5-1" in result.unique().get("sub_collection", {"values": []})["values"]:
        result = result.search(sub_collection="era5-1")

    logging.debug(f"Opening {result.df.path.values}")

    d = result.to_dataset_dict(cdf_kwargs={"chunks": chunks}, progressbar=False)

    if len(d) > 1:
        raise Exception(f"Multiple matches: {kwargs} {d.keys()}")

    ds = list(d.values())[0]

    da = ds[ds.attrs["intake_esm_varname"][0]]

    params = paramdb()
    params = params[params.cfVarName == da.name]

    if len(params) == 0:
        return da

    params = params.iloc[0]

    da.attrs["table"] = numpy.int32(params["table2Version"])
    da.attrs["code"] = numpy.int32(params["indicatorOfParameter"])
    da.attrs["standard_name"] = params["cfName"]
    da.attrs["ecmwf_name"] = params["name"]
    da.attrs["ecmwf_shortname"] = params["shortName"]
    da.attrs["units"] = params["units"]

    return da


def read_era5_land(var, year, month):
    """
    Read land values (must be interpolated to era5 grid)
    """
    cat = intake.cat.nci.era5_land
    da = load_var(
        cat, parameter=var, year=year, month=month, product_type="reanalysis"
    )  # , chunks={"time": 1, 'latitude': 500, 'longitude': 500},)
    da.name = da.name + "_land"
    return da


def read_era5_surface(var, year, month):
    """
    Read surface values
    """
    cat = intake.cat.nci.era5
    da = load_var(
        cat,
        parameter=var,
        year=year,
        month=month,
        product_type="reanalysis",
        dataset="single-levels",
    )
    da.name = da.name + "_surf"
    return da


def read_era5_pressure(var, year, month):
    """
    Read pressure level values
    """
    cat = intake.cat.nci.era5
    da = load_var(
        cat,
        parameter=var,
        year=year,
        month=month,
        product_type="reanalysis",
        dataset="pressure-levels",
        chunks={"time": 12, "level": 1},
    )
    da.name = da.name + "_pl"
    return da


@functools.lru_cache
def regrid():
    """
    Create regridding from land to surface
    """
    land = read_era5_land("2t", 2000, 1)
    surf = read_era5_surface("2t", 2000, 1)
    return xesmf.Regridder(
        land[0, ...],
        surf[0, ...],
        "bilinear",
        filename=resource_filename(__name__, "nci_regrid_weights.nc"),
        reuse_weights=True,
    )


def merged_land_surf(var, year, month):
    """
    Read the land and surface values for a variable, composing them so that
    over land the land values are used, and over ocean the surface values are
    used
    """
    lsm = read_era5_surface("lsm", 2000, 1)[0, :, :]
    surf = read_era5_surface(var, year, month)

    # Different names in era5 and era5-land
    renamed_vars = {"10u": "u10", "10v": "v10"}

    if var in ["ci", "msl", "sst"] or year < 1981:
        # No land value
        return surf

    if var.startswith("stl") or var.startswith("swvl"):
        # Don't merge with surf
        land = read_era5_land(var, year, month)
        land_on_surf = regrid()(land)

    else:
        # Merge surf over ocean with land
        land = read_era5_land(renamed_vars.get(var, var), year, month)

        if (year, month) == (1981, 1):
            # Fill in 19810101T0000 with values from 19810101T0100
            surf, land = xarray.align(
                surf, land, join="left", exclude=["latitude", "longitude"]
            )
            land = land.bfill("time", limit=None)

        land_on_surf = regrid()(land) * lsm + surf * (1 - lsm)

    land_on_surf.name = surf.name
    land_on_surf.attrs = surf.attrs

    return land_on_surf


def read_era5_month(surface_vars, pressure_vars, year, month, era5land: bool = True):
    """
    Read a collection of surface and pressure level values for a single month
    """
    if era5land:
        surf = [merged_land_surf(v, year, month) for v in surface_vars]
    else:
        surf = [read_era5_surface(v, year, month) for v in surface_vars]

    plev = [read_era5_pressure(v, year, month) for v in pressure_vars]

    return xarray.merge([*surf, *plev])


def read_era5(surface_vars, pressure_vars, start, end, era5land: bool = True):
    """
    Read a collection of surface and pressure level values between start and end
    """

    if start < pandas.Timestamp("1979-01-01T00:00"):
        raise ValueError(
            f"Start time {start} is before ERA5 start date 1979-01-01T00:00"
        )

    t0 = pandas.offsets.MonthBegin().rollback(start.date())
    t1 = pandas.offsets.MonthEnd().rollforward(end.date())

    result = []

    for t in pandas.date_range(t0, t1, freq="M"):
        result.append(
            read_era5_month(
                surface_vars, pressure_vars, t.year, t.month, era5land=era5land
            )
        )

    ds = xarray.concat(result, dim="time")

    ds["lsm"] = read_era5_surface("lsm", 2000, 1).isel(time=0)  # .squeeze('time')
    ds["z"] = read_era5_surface("z", 2000, 1).isel(time=0)  # .squeeze('time')

    return ds.sel(time=slice(start, end))


def read_um(time, era5land: bool = True):
    # Make sure the time includes an hour
    start = pandas.offsets.Hour().rollback(time)
    ds = read_era5(
        [
            "skt",
            "sp",
            "ci",
            "sd",
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
        era5land=era5land,
    )

    for k, v in ds.items():
        ds[k] = v.fillna(v.mean())

    ds = soil_level_metadata(ds)

    return ds


def read_wrf(start, end, era5land: bool = True):
    ds = read_era5(
        [
            "10u",
            "10v",
            "2t",
            "2d",
            "sp",
            "msl",
            "skt",
            "ci",
            "sst",
            "rsn",
            "sd",
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
        era5land=era5land,
    )

    ds = soil_level_metadata(ds)

    for v in ds.values():
        if v.dtype == "float64":
            v.encoding["dtype"] = "float32"
        if v.dtype == "int64":
            v.encoding["dtype"] = "int32"

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

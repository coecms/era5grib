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


fx = xarray.open_dataset("/g/data/ub4/era5/netcdf/static_era5.nc").isel(time=0)
fx = fx.sel(latitude=slice(20, -57), longitude=slice(78, 220))
fx.lsm.attrs["code"] = numpy.int32(172)
fx.z.attrs["code"] = numpy.int32(129)

chunks = {
    "surface": {"time": 1, "latitude": 91 * 2, "longitude": 180 * 2},
    "land": {"time": 1, "latitude": 129 * 4, "longitude": 258 * 4},
    "pressure": {"time": 1, "latitude": 39 * 4, "longitude": 72 * 4, "level": 4},
}


catalogue = None
regridder = None  # Regridder(weights=weights)


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

    for ms, me in zip(
        pandas.date_range(t0, t1, freq="MS"), pandas.date_range(t0, t1, freq="M")
    ):
        pattern = os.path.join(
                entry["dirname"],
                ms.strftime("%Y"),
                f'*_{ms.strftime("%Y%m%d")}_{me.strftime("%Y%m%d")}.nc',
            )
        path = glob(pattern)
        paths.extend(path)

    try:
        da = xarray.open_mfdataset(paths, chunks=chunks[product])[var]
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

    print("Selecing ERA5 archive files")
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
    with tempfile.NamedTemporaryFile() as tmp:
        print("Creating intermediate file")
        tmpnc = tmp.name
        climtas.io.to_netcdf_throttled(ds, tmpnc)
        print("Converting to GRIB")
        subprocess.run(
            ["cdo", "-f", "grb1", "-t", "ecmwf", "copy", tmpnc, output], check=True
        )


def read_um(time):
    # Make sure the time includes an hour
    start = pandas.offsets.Hour().rollback(time)
    ds = read_era5(
        [
            "skt",
            "sp",
            #"siconc",
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

    for k,v in ds.items():
        print(k)
        ds[k] = v.fillna(v.mean())

    return ds


def read_wrf(start, end):
    ds = read_era5(
        [
            "u10",
            "v10",
            "t2m",
            "d2m",
            "sp",
            "msl",
            "skt",
            #"siconc",
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

    return ds

def select_domain(ds, lats, lons):
    error = False
    message = "ERROR: Target area is outside the ERA5 archive domain"

    if lats.max() > ds.latitude[0] or lats.min() < ds.latitude[-1]:
        error = True
        message += f"\n    Latitudes: Target ({lats.min().values:.2f}:{lats.max().values:.2f}), ERA5 (-57:20)"
    if lons.min() < ds.longitude[0] or lons.max() > ds.longitude[-1]:
        error = True
        message += f"\n    Longitudes: Target ({lons.min().values:.2f}:{lons.max().values:.2f}), ERA5 (78:220)"

    if error:
        raise IndexError(message)

    return ds.sel(latitude=slice(lats.max()+1, lats.min()-1),
            longitude=slice(lons.min()-1, lons.max()+1))




def era5grib_wrf(start=None, end=None, output=None, namelist=None, geo=None):
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

    ds = read_wrf(start, end)

    if geo is not None:
        geo = xarray.open_dataset(geo)
        ds = select_domain(ds, lats=geo.XLAT_M, lons=geo.XLONG_M)

    else:
        print("WARNING: Outputting the full domain, use --geo=geo_em.d01.nc to limit")

    save_grib(ds, output)

    print(f"Wrote {output}")


def era5grib_um(time, output=None, target=None):
    """
    Convert the NCI ERA5 archive data to GRIB format for use in UM limited area
    modelling.

    Will generate a grib file for a single time, to be processed by the UM
    reconfiguration.

    The output area can be limited to reduce file size by supplying a UM file
    on the target grid as 'target'
    """

    ds = read_um(time)

    if output is None:
        output = f"um.era5.{pandas.to_datetime(ds.time.values[0]).strftime('%Y%m%dT%H%M')}.grib"

    if target is not None:
        cube = iris.load(target)[0]

        lat = cube.coord("latitude").points
        lon = cube.coord("longitude").points

        if (
            lat.max() > ds.latitude[0]
            or lat.min() < ds.latitude[-1]
            or lon.min() < ds.longitude[0]
            or lon.max() > ds.longitude[-1]
        ):
            raise IndexError("Selected domain is outside the NCI ERA5 archive")

        ds = ds.sel(
            latitude=slice(lat.max() + 1, lat.min() - 1),
            longitude=slice(lon.min() - 1, lon.max() + 1),
        )
    else:
        print("WARNING: Outputting the full domain, use --target=qrparm.mask to limit")

    save_grib(ds, output)

    print(f"Wrote {output}")


def init():
    global catalogue
    catalogue = pandas.read_csv(
        resource_stream(__name__, "catalogue.csv"), index_col=["product", "varname"]
    )
    weights = xarray.open_dataset(resource_filename(__name__, "regrid_weights.nc"))

    global regridder
    regridder = Regridder(weights=weights)


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
    wrf.add_argument("--start", help="Output start time")
    wrf.add_argument("--end", help="Output end time")
    wrf.add_argument("--output", help="Output file")
    wrf.add_argument("--geo", help="Geogrid file for trimming (e.g. geo_em.d01.nc)")

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

    args = parser.parse_args()

    init()

    dargs = vars(args)
    func = dargs.pop("func")

    func(**dargs)


if __name__ == "__main__":
    main()

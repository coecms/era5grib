import numpy
import xarray as xr
import mule

from pathlib import Path
from typing import Optional, Tuple

from .logging import log, die

def domain_from_ds(ds: xr.Dataset, polar:bool) -> Tuple[numpy.array,numpy.array]:

    if not polar:
        lons = ds.XLONG_M.where(ds.XLONG_M > 0, ds.XLONG_M + 360).values
    else:
        lons = numpy.array([0, 359.75])
    return ds.XLAT_M.values, lons

def domain_from_um(mf, polar:bool) -> Tuple[numpy.array,numpy.array]:
    ny = mf.integer_constants.num_rows
    nx = mf.integer_constants.num_cols

    y0 = mf.real_constants.start_lat
    x0 = mf.real_constants.start_lon

    dy = mf.real_constants.row_spacing
    dx = mf.real_constants.col_spacing

    lat = y0 + numpy.arange(ny) * dy
    if not polar:
        lon = x0 + numpy.arange(nx) * dx
    else:
        lon = numpy.array([0, 359.75])

    return lat,lon

def get_domain(fn: Optional[Path], polar: bool) -> Tuple[slice,slice]:
    """
    Return the model domain from a given file path. Accepts either
    WRF geo_em files or UM.
    """

    lats=numpy.array([-90,90])
    lons=numpy.array([0,359.75])

    found=False

    if fn:
        try:
            log.info("Attempting to open domain as netCDF file with Xarray")
            ds = xr.open_dataset(fn,engine="netcdf4")
        except FileNotFoundError:
            log.error(f"Input domain data file not found")
        except AttributeError:
            log.error(f"Input dataset is not a valid geogrid file")
        except OSError:
            log.info("Not Found")
            pass
        else:
            log.info("Found")
            lats,lons=domain_from_ds(ds,polar)
            found=True

        if not found:
            try:
                log.info("Attempting to open domain as UM file with mule")
                mf = mule.load_umfile(str(fn))
            except ValueError:
                die(f"Invalid input file for domain: {fn}")
            log.info("Found")
            lats,lons=domain_from_um(mf,polar)

        log.info(f"Latitudes: Target ({lats.min():.2f}:{lats.max():.2f})")
        log.info(f"Longitudes: Target ({lons.min():.2f}:{lons.max():.2f})")

        ### Return a slightly bigger region so the interpolation goes OK
        ### An extra degree should be sufficient
        lat_min = lats.min() - 1
        if lat_min <= -90.0:
            lat_min = None

        lat_max = lats.max() + 1
        if lat_max >= 90.0:
            lat_max = None

        lon_min = lons.min() - 1
        if lon_min <= 0.0:
            lon_min = None

        lon_max = lons.max() + 1
        if lon_max >= 359.75:
            lon_max = None

        ### Lats are backwards in ERA5
        return slice(lat_max, lat_min),slice(lon_min,lon_max)
    else:
        log.warn("Outputting the global domain - use qrparm.mask (for UM) or Geogrid file (for WRF) to restrict to limited area")
        return slice(None), slice(None)

def get_domain_with_buffer(lat_range: slice, lon_range: slice) -> Tuple[slice,slice]:
    
    ### Lats are backwards in ERA5
    lat_min = lat_range.stop
    if lat_min:
        if lat_min <= -89.0:
            lat_min = None
        else:
            lat_min = lat_min - 1

    lat_max = lat_range.start
    if lat_max:
        if lat_max >= 89.0:
            lat_max = None
        else:
            lat_max = lat_max + 1

    lon_min = lon_range.start
    if lon_min:
        if lon_min <= 1:
            lon_min = None
        else:
            lon_min = lon_min - 1

    lon_max = lon_range.stop
    if lon_max:
        if lon_max >= 358.75:
            lon_max = None
        else:
            lon_max = lon_max + 1

    ### Lats are backwards in ERA5
    return slice(lat_max, lat_min),slice(lon_min,lon_max)


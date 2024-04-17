import numpy
import xarray as xr
import mule

from pathlib import Path
from typing import Optional, Tuple

from .logging import log



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

    if fn:
        try:
            ds = xr.open_dataset(fn,engine="netcdf4")
        except FileNotFoundError:
            log.error(f"Input domain data file not found")
        except AttributeError:
            log.error(f"Input dataset is not a valid geogrid file")
        except OSError:
            pass
        else:
            lats,lons=domain_from_ds(ds,polar)
    
        try:
            mf = mule.load_umfile(str(fn))
        except ValueError:
            log.error(f"Invalid input file for domain: {fn}")
            exit(-1)
        lats,lons=domain_from_um(mf,polar)

        log.info(f"Latitudes: Target ({lats.min():.2f}:{lats.max():.2f})")
        log.info(f"Longitudes: Target ({lons.min():.2f}:{lons.max():.2f})")

        return slice(lats.max() + 1, lats.min() - 1),slice(lons.max()+1,lons.min() - 1)

    else:
        log.warn("Outputting the global domain - use qrparm.mask (for UM) or Geogrid file (for WRF) to restrict to limited area")
        return slice(None), slice(None)

import intake_esm
import numpy as np
import xarray as xr
from ..logging import log

from typing import Dict

def decode_mask(da: xr.DataArray) -> xr.DataArray:
    fill_vals = set()
    if "_FillValue" in da.attrs:
        fill_vals.add(da.attrs["_FillValue"])
        log.debug(f"_FillValue found: {da.attrs['_FillValue']}")
        del(da.attrs['_FillValue'])

    if "missing_value" in da.attrs:
        fill_vals.add(da.attrs["missing_value"])
        log.debug(f"missing_value found: {da.attrs['missing_value']}")
        del(da.attrs['missing_value'])

    if len(fill_vals) > 1:
        log.warn(f"{da.name} Field has multiple fill values - setting all to NaN")

    for fv in fill_vals:
        da = da.where(da!=fv)

    return da


def cat_to_dataset_dict(result: intake_esm.core.esm_datastore,chunks: Dict[str,int]):

    dataset_dict = result.to_dataset_dict(xarray_open_kwargs={'chunks':chunks,"mask_and_scale":False},progressbar=False)
    for ds in dataset_dict.values():
        for da in ds:

            ds[da] = decode_mask(ds[da])

            if "scale_factor" not in ds[da].attrs and "add_offset" not in ds[da].attrs:
                log.debug(f"{da} has no scale_factor or add_offset - skipping")
                continue

            if "scale_factor" in ds[da].attrs:
                scale_factor = ds[da].attrs["scale_factor"]
                log.debug(f"scale_factor found: {ds[da].attrs['scale_factor']}")
                del(ds[da].attrs["scale_factor"])
            else:
                scale_factor = 1.0
            if "add_offset" in ds[da].attrs:
                offset = ds[da].attrs["add_offset"]
                log.debug(f"add_offset found: {ds[da].attrs['add_offset']}")
                del(ds[da].attrs["add_offset"])
            else:
                offset = 0.0

            attrs=ds[da].attrs
            ### These operations drop attributes
            ds[da] = ds[da].astype(np.float32) * scale_factor + offset
            ds[da].attrs = attrs

    return dataset_dict
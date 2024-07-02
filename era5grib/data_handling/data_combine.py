import xarray as xr
import pandas
import xesmf

from ..config import conf
from ..logging import log,die
from .data_read import get_single_field
from .grib_metadata import Paramdb
from .era5field import Era5field

from pathlib import Path
from pandas import Timestamp
from collections import OrderedDict

from typing import Dict, Tuple

class InterpolatingRegridder():
    """
    A regridding class that first uses xr.interpolate_na to perform a linear interpolation
    across all NaN points in the grid before performing the actual xesmf interpolation.
    Used to handle the interaction between NaN ocean points in the ERA5-Land data and the
    fractional ERA5 landmask in a more adaptable way than a constant weights file. Fills
    NaN corner points with field average and calls interpolate_na twice on each
    field, vertically then horizontally on one, and reversed on the other. The final
    field is given by averaging the two filled fields, then interpolated.
    """
    def __init__(self,*args,**kwargs):
        self.regridder = xesmf.Regridder(*args,**kwargs)
    
    def __call__(self,field):
        ### Set corners to field mean if they're NaN
        fmean = field.mean(dim=("latitude","longitude"))
        out = field.chunk({'latitude':-1,'longitude':-1})
        for lat in (0,-1):
            for lon in (0,-1):
                out = xr.where( (out.latitude == out.latitude[lat]) & (out.longitude == out.longitude[lon]) & out.isnull(),fmean,out)
        lat_first = out.interpolate_na(dim='latitude',method='bilinear',use_coordinate=False).interpolate_na(dim='longitude',method='bilinear',use_coordinate=False)
        lon_first = out.interpolate_na(dim='longitude',method='bilinear',use_coordinate=False).interpolate_na(dim='latitude',method='bilinear',use_coordinate=False)

        return self.regridder((lat_first + lon_first) / 2)

def merge_fields_in_time(fields: Dict[Timestamp,Dict[Tuple[str,str],Era5field]]) -> Dict[Tuple[str,str],Era5field]:
    
    custom_fields = [ i for i in conf.get("custom_fields",{}).values()]
    static_fields = conf.get('static',{})
    
    fields_to_merge = OrderedDict()

    ts = conf.get_month_range()[0]
    d = fields[ts]

    for (field_name,ds),field in d.items():
        key=(field_name,ds)
        fields_to_merge[key] = Era5field(field_name)
        for realm,da in field.get_dataarrays():
            if realm in fields_to_merge[key]:
                die(f"Error: Multiple definition of {field_name} on {realm}")
            ### Custom fields only need to be handled on the first time through
            if da.attrs["source"] in custom_fields:
                if "time" in da.coords:
                    ### Already handled
                    fields_to_merge[key].add_dataarray(da,realm)
                else:
                    fields_to_merge[key].add_dataarray(da.expand_dims({"time":conf.get_time_range()}),realm)
                continue
            #if ds in static_fields:
                #if field_name in static_fields[ds]:
            if conf.get(f'static.{ds}.{field_name}') is not None:
                ### Static field - only include first timestep
                fields_to_merge[key].add_dataarray(da.sel(time=conf.get("start")),realm)
                continue
            fields_to_merge[key].add_dataarray(da.sel(time=conf.get_time_range()),realm)

    for ts in conf.get_month_range()[1:]:
        for (field_name,ds),field in fields[ts].items():
            key=(field_name,ds)
            if key not in fields_to_merge:
                die(f"Error: {key} in timestamp {ts} of catalogue search results, but not in first timestep {conf.get_month_range()[0]}")
            for realm,da in field.get_dataarrays():
                if da.attrs["source"] in custom_fields:
                    continue
                if ds in static_fields:
                    if field_name in static_fields[ds]:
                        continue
                if realm not in fields_to_merge[key]:
                    die(f"Error: {key} on {realm} in timestamp {ts} of catalogue search results but not in first timestep {conf.get_month_range()[0]}")
                fields_to_merge[key].concat_dataarray(da.sel(conf.get_time_range()),realm)
    
### Sanity checks - all timestemps need to have the exact same number of
### variables and the same number of data arrays for each variable
    time_dim=None
    for (field_name,ds),field in fields_to_merge.items():
        for realm,da in field.get_dataarrays():
            if realm in static_fields:
                if field_name in static_fields[realm]:
                    continue
            if time_dim is None:
                time_dim = da.time
            if not da.time.equals(time_dim):
                ### If this is a custom field, just align it
                if da.attrs["source"] in custom_fields:
                    if len(time_dim) != len(da.time):
                        die(f"Error! Custom field {field_name} has different time dimension length than remaining data: {len(time_dim)}, {len(da.time)}")
                    da = da.assign_coords(time=time_dim)
                    continue
                if ds in static_fields:
                    if field_name in static_fields[ds]:
                        continue
                die(f"Error: Time dimension mismatch in field {field_name}")

    return fields_to_merge

def handle_regridding(fields: Dict[Tuple[str,str],Era5field]) -> None:

### Now regrid if necessary
    regrid_options = conf.get("regrid_options")
    regrid = conf.get("regrid")
    example_das = {}
    regridders = {}
    for field in fields.values():
        for _,da in field.get_dataarrays():
            if da.attrs["source"] not in example_das:
                ### If regrid is not specified, pick a the first
                ### da we find as the source
                if regrid is None: regrid = da.attrs["source"]
                example_das[da.attrs["source"]] = da.isel(time=0)
                if "level" in example_das[da.attrs["source"]].coords:
                    example_das[da.attrs["source"]] = example_das[da.attrs["source"]].isel(level=0).drop_vars('level')
    target_da = None
    if regrid in example_das:
        target_da = example_das[regrid]
    else:
        ### Regridding to unloaded data array
        #regrid_params = conf.get("regridding") or {}
        ref_field=conf.get("regrid_params.ref_field")
        ref_date=conf.get("regrid_params.ref_date")
        #if "ref_field" not in regrid_params or "ref_date" not in regrid_params:
        if ref_field is None or ref_date is None:
            die("Regridding to unloaded dataarray requested, but regridding parameters 'ref_field' and 'ref_date' are not set")
        target_da = get_single_field(ref_field,regrid,pandas.Timestamp(ref_date))
    if target_da is None:
        die(f"Could not find unloaded dataarray to regrid to: {ref_field}")

    for source,da in example_das.items():
        if da.latitude.equals(target_da.latitude) and da.longitude.equals(target_da.longitude):
            regridders[source] = None
            ### Can we use any existing regridders?
        else:
            for other_source, other_da in example_das.items():
                if other_source == source:
                    continue
                if da.latitude.equals(other_da.latitude) and da.longitude.equals(other_da.longitude):
                    regridders[source] = regridders[other_source]
            if regrid is None:
                die(f"Error! Regridding not specified and field {da.name} has mismatching grid")
            if regrid_options == "weight_file":
                weight_file = str(Path(__file__).parent.parent / "nci_regrid_weights.nc")
                regridders[source] = xesmf.Regridder(da.to_dataset(),target_da.to_dataset(),"bilinear",filename=weight_file,reuse_weights=True)
            else:
                regridders[source] = InterpolatingRegridder(da.to_dataset(),target_da.to_dataset(),"bilinear")

    if regrid_options == "weight_file":
        ### Special case, can only regrid from era5land -> era5
        for source, regridder in regridders.items():
            if regridder is not None:
                if len(example_das[source].latitude) != 1801 or len(example_das[source].longitude) != 3600:
                    die("ERROR: weight_file regridding option can only be used to regrid from era5land to era5")

    if regrid:
        for field in fields.values():
            for realm,da in field.get_dataarrays():
                field.set_regridder(realm,regridders[da.attrs["source"]])
            field.regrid()

def combine(fields: Dict[Timestamp,Dict[Tuple[str,str],Era5field]]) -> xr.Dataset:
    """
This function takes a list of Dict of Era5field objects. Each dict kv
pair corresponds to a month of ERA5 data. Custom data
can have no time dimension or can contain every timestep in the model
Therefore, custom data only needs to be dealt with on the first month
"""
    regrid = conf.get("regrid")

    fields_to_merge = merge_fields_in_time(fields)
    handle_regridding(fields_to_merge)

    #land_masks = conf.get('land-mask') or {}
    ### Don't need a landmask if there is no merging to do
    if all([ len(i)==1 for i in fields_to_merge.values() ]):
        log.info("No merging required")
        land_mask_da = None
    else:
        log.info("Retrieving landmask for field merging")
        if conf.get('land-mask') is None:
            die("Error! DataArray merge is required and no land mask has been specified")
        if regrid:
            land_mask_name=conf.get(f"land-mask.{regrid}")
            log.debug(f"Regridding required - using {land_mask_name} from land-mask.{regrid}")
            #if regrid in land_masks:
            #    land_mask_name = land_masks[regrid]
        else:
            log.debug("Not regridding - looking for first land-mask field")
            land_mask_name = [ i for i in conf.get('land-mask').values() ][0]
        if (land_mask_name,"single-levels") in fields_to_merge:
            _,land_mask_da = next(fields_to_merge[(land_mask_name,"single-levels")].get_dataarrays())
        else:
            ### Need to load a field
            land_mask_source = regrid or [ i for i in fields_to_merge.values() ][0].attrs['source']
            land_mask_da = get_single_field(land_mask_name,land_mask_source,conf.get("start"))
        if land_mask_da is None:
            die("Unable to recover landmask for merging dataarrays")
        
    ### Run merge on everything though, as it does do nothing for 
    ### era5fields with a single dataarray
    for (_,ds_type),field in fields_to_merge.items():
        field.merge(land_mask_da,ds_type)

    ds = xr.merge([v.get_merged_field() for v in fields_to_merge.values()])

    ### Add grib metadata here
    grib_params = Paramdb()
    for k in ds:
        ds[k].attrs |= grib_params(k)

    return soil_level_metadata(ds)

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
        ds.coords[f"depth{l}"] = xr.DataArray(
            depth[l : (l + 1)], dims=[f"depth{l}"], attrs=depth_attrs
        )
        ds.coords[f"depth{l}_bnds"] = xr.DataArray(
            [depth_bnds[l : (l + 2)]], dims=[f"depth{l}", "bnds"], attrs=depth_attrs
        )
        ds.coords[f"depth{l}"].attrs["bounds"] = f"depth{l}_bnds"

    return ds
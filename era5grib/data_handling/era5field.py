import xarray as xr

from ..config import conf
from ..logging import die
from collections import OrderedDict

from typing import Tuple, Generator, Callable

_default_regridder = lambda x: x

class Era5field:

    def __init__(self,name):

        self.name = name
        self.data_arrays = OrderedDict()
        self.data_array_to_merge = None
        self.regridders = {}
    
    def __contains__(self,realm: str):
        return realm in self.data_arrays

    def __len__(self) -> int:
        return len(self.data_arrays) 

    def add_dataarray(self,da: xr.DataArray,realm: str) -> None:
        self.data_arrays[realm] = da
        self.regridders[realm] = _default_regridder
        ### Reset encoding attribute
        self.data_arrays[realm].encoding = {}

    def get_dataarrays(self) -> Generator[Tuple[str,xr.DataArray],None,None]:
        for realm,da in self.data_arrays.items():
            yield realm,da
    
    def set_regridder(self,realm: str, regridder: Callable) -> None:
        self.regridders[realm] = regridder
    
    def is_complete(self) -> bool:
        if "global" in self.data_arrays:
            return True
        if "land_only" in self.data_arrays and self.name in conf.get("land_only") or {}:
            return True
        if "ocean_only" in self.data_arrays and self.name in conf.get("ocean_only") or {}:
            return True
        if "land_only" in self.data_arrays and "ocean_only" in self.data_arrays:
            return True
        return False
    
    def regrid(self) -> None:
        for realm in self.data_arrays:
            if self.regridders[realm] is None:
                continue
            name = self.data_arrays[realm].name
            attrs = self.data_arrays[realm].attrs
            dropped_time = False
            if len(self.data_arrays[realm].time) == 1:
                ### Drop 'scalar' time or xarray complains
                dropped_time = True
                td = self.data_arrays[realm].time
                self.data_arrays[realm] = self.data_arrays[realm][0].drop_vars("time")
            self.data_arrays[realm] = self.regridders[realm](self.data_arrays[realm])
            if dropped_time:
                self.data_arrays[realm] = self.data_arrays[realm].expand_dims({'time':td})
            self.data_arrays[realm].name  = name
            self.data_arrays[realm].attrs = attrs

    def merge(self,land_mask: xr.DataArray,ds_type: str) -> None:
        ### Trim the field down to the requested region here
        lat_range, lon_range = conf.get("domain")
        #ds_tags = conf.get('dataset_tags') or {}
        #static_fields = conf.get('static') or {}
        ds_tag = conf.get(f'dataset_tags.{ds_type}')

        if len(self.data_arrays) == 1:
            self.data_array_to_merge = next(iter(self.data_arrays.values())).sel(latitude=lat_range,longitude=lon_range)
            name = self.data_array_to_merge.name
            if name not in conf.get(f'static.{ds_type}',[]):
                if ds_tag is not None:
                    self.data_array_to_merge.name = name+"_"+ds_tag

            #if ds_type in ds_tags:
            #    if ds_type in static_fields:
            #        if name not in static_fields[ds_type]:
            #            self.data_array_to_merge.name = name+"_"+ds_tags[ds_type]
            #    else:
            #        self.data_array_to_merge.name = name+"_"+ds_tags[ds_type]
            return

        ### Do we want the user to be able to configure landmask handling?
        lm = land_mask.sel(latitude=lat_range,longitude=lon_range)
        ### Record whether the previous contribution to the merged field was weighted by the landmask
        prev_weighted = True
        for realm,da in reversed(self.data_arrays.items()):
            ### Any remaining field is defined on the whole globe, so
            ### Any nan's will cause issues issues when combining fields
            ### fill nan's with the field average before attempting to merge
            da = da.sel(latitude=lat_range,longitude=lon_range)
            #da = da.fillna(da.mean(dim=('latitude','longitude')))
            if self.data_array_to_merge is None:
                self.data_array_to_merge = xr.zeros_like(da)
                attrs = da.attrs
                name = da.name
            if realm == "ocean_only":
                ### Previous contributions are land-only
                if prev_weighted:
                    self.data_array_to_merge = self.data_array_to_merge + da * (1-lm)
                else:
                    self.data_array_to_merge = self.data_array_to_merge*lm + da * (1-lm)
                prev_weighted=True
            if realm == "land_only":
                ### Previous contributions are ocean-only
                if prev_weighted:
                    self.data_array_to_merge = self.data_array_to_merge + da * land_mask
                else:
                    self.data_array_to_merge = self.data_array_to_merge*(1-lm) + da * lm
                prev_weighted=True
            if realm == "global":
                ### Previous contributions are clobbered.
                self.data_array_to_merge = da
                prev_weighted=False
        self.data_array_to_merge.attrs = attrs
        if ds_tag is not None:
            self.data_array_to_merge.name = name+"_"+ds_tag
        else:
            self.data_array_to_merge.name = name
        
    def concat_dataarray(self,da: xr.DataArray,realm: str) -> None:
        
        if realm not in self.data_arrays:
            die(f"Error! Attempted to concatenate non-existent data array: {self.name} on {realm}")
        self.data_arrays[realm] = xr.concat([self.data_arrays[realm],da],"time")
    
    def get_merged_field(self) -> xr.DataArray:
        return self.data_array_to_merge
import xarray as xr
from ..config import conf

def write(ds: xr.Dataset):

    ds.time.encoding["units"] = "hours since 1970-01-01"
    ### Correct chunking if we know what the chunks should be
    if 'source' in ds.attrs:
        chunkspec=conf.get(f"catalogue_flags.{ds.attrs['source']}.chunks","auto")
    else:
        chunkspec="auto"
        chunks = None
    encoding={}
    for field_name in ds.keys():
        if chunkspec != 'auto':
            chunks = []
            for k in ds[field_name].dims:
                dim_size = len(ds[field_name].coords[k])
                c = chunkspec.get(k,-1)
                if dim_size >= c and c > 0:
                    chunks.append(c)
                else:
                    chunks.append(dim_size)
        else:
            chunks=None
        encoding[field_name]={"chunksizes":chunks} | ds[field_name].encoding

    ds.to_netcdf(conf.get("output"),encoding=encoding)
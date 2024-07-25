import xarray as xr
import intake
import intake_esm

from pandas import Timestamp
from .era5field import Era5field
from .xarray_legacy_read import cat_to_dataset_dict
from ..config import conf
from ..logging import log, die

from collections import OrderedDict, namedtuple
from typing import Dict,Tuple,Optional,List,NamedTuple,Union

_lat_names = ["latitude", "lat","LAT","LATITUDE", "Lat","Latitude" ]
_lon_names = ["longitude","lon","LON","LONGITUDE","Lon","Longitude"]

def find_datasets(cat: intake_esm.core.esm_datastore, datasets: List[str], name: str) -> List[intake_esm.core.esm_datastore]:

    sub_cats=[]

    if "dataset" in cat.df:
        for intake_ds in datasets:
            log.debug(f"Searching for dataset {intake_ds}")
            sub_cat = cat.search(dataset=intake_ds)
            if len(sub_cat.df) > 0:
                ### Need to keep hold of the names
                sub_cat.name = name
                log.debug(f"Appending catalogue: {sub_cat}")
                sub_cats.append(sub_cat)
            else:
                log.debug("Not Found")
    else:
        ### Need to keep hold of the names
        cat.name = name
        sub_cats.append(cat)
    return sub_cats

def get_catalogues() -> List[Union[intake_esm.core.esm_datastore,NamedTuple]]:

    custom_field_cat_key = conf.get("custom_field_catalogue_key")
    datasets = [ k for k in conf.get("fields").keys() ]
    cats = []
    for cat_path in conf.get('catalogue_paths'):
        log.info(f"Trying catalogue path: {cat_path}")
        for cat in conf.get('catalogues'):
            if cat == custom_field_cat_key:
                ### We found the special (fake) "custom field" catalogue, create an empty
                ### object that has a 'name' attribute, we'll need to query that later
                out = namedtuple("FakeCatalogue","name")
                out.name = custom_field_cat_key
                cats.append(out)
                log.debug(f"Skipping custom field {custom_field_cat_key} placeholder")
                continue
            log.debug(f"Looking for {cat} in {cat_path}")
            c = intake.open_catalog(cat_path)
            try:
                out = c[cat]
                log.debug(f"Found")
            except KeyError:
                log.debug(f"Not Found")
                continue
            product_type = conf.get(f'catalogue_flags.{out.name}.product_type')
            log.debug(f"product_type: {product_type}")
            if product_type:
                n = out.name
                log.info(f"Filtering by product type: {product_type}")
                out = out.search(product_type=product_type)
                out.name = n
            sub_coll_pref = conf.get(f'catalogue_flags.{out.name}.sub_collection_pref')
            log.debug(f"sub_coll_pref: {sub_coll_pref}")
            if sub_coll_pref is not None:
                log.info(f"Attempting to find preferred subcollection: {sub_coll_pref}")
                sub_cat = out.search(sub_collection=sub_coll_pref)
                if len(sub_cat.df) > 0:
                    log.debug("Found")
                    sub_sub_cat = find_datasets(sub_cat,datasets,sub_coll_pref)
                    cats.extend(sub_sub_cat)
            if len(out.df) > 0:
                sub_cat = find_datasets(out,datasets,out.name)
                cats.extend(sub_cat)
    if not cats:
        die("No valid catalogues specified")
    
    return cats

def get_single_field(field_name: str, source: str, ts: Timestamp) -> Optional[xr.DataArray]:
    
    log.info(f"Retrieving single field {field_name} from {source}")
    lat_buffer_range, lon_buffer_range = conf.get("domain_with_buffer")
    ### Source is a file.
    if source.startswith("/"):
        log.debug(f"{source} is file")
        ds = xr.open_dataset(source)
        if field_name in ds:
            log.debug(f"{field_name} found")
            return ds[field_name][0].drop_vars("time").sel(latitude=lat_buffer_range,longitude=lon_buffer_range)
        else:
            log.debug(f"{field_name} not found")
            return None
    log.debug(f"{source} is catalogue")
    for cat in get_catalogues():    
        if cat.name() == source:
            log.debug(f"{source} found")
            result = cat.search(parameter=field_name,year=ts.year,month=ts.month)
            return xr.open_dataarray(result.path[0])[0].drop_vars("time").sel(latitude=lat_buffer_range,longitude=lon_buffer_range)
    log.debug(f"{source} not found")
    return None


def handle_custom_field(field_name: str, file_name: str) -> xr.DataArray:
    """
    Handle custom fields to load in place of standard catalogue fields
    The rules for files containing these fields are: 
    1) A file must either:
       a) have a single timestamp; OR 
       b) contain a timestamp that matches the current model time
    2) A file must either:
       a) have a single field; OR
       b) contain a field that matches the name given in the custom-fields
          configuration key

    At this stage we can only do custom surface fields.
    """
    log.info(f"Searching for custom field {field_name} in {file_name}")
    try:
        ds = xr.open_dataset(file_name)
        log.debug("Opened dataset")
    except FileNotFoundError:
        die(f"File {file_name} for field {field_name} could not be found")
    
    ### Check data vars
    varlist = [ i for i in ds.variables if i not in ds.coords ]
    log.debug(f"Vars in file: {varlist}")
    if not varlist:
        die(f"{file_name} contains no variables")
    if len(varlist) > 1:
        log.debug(f"File has more than one var")
        try:
            da = ds[field_name]
            log.debug("Dataset contains required var")
        except KeyError:
            die(f"{file_name} contains multiple variables, none of which are {field_name}")
    else:
        log.debug(f"File has one var")
        da = ds[varlist[0]]
    
    ### Check times
    if "time" in da.coords:
        log.debug("Dataset has time coord")
        if da.time.size > 1:
            log.debug("File has more than one time point")
            tr = conf.get_time_range()
            try:
                da = da.sel(time=tr)
                log.debug("Dataset contains required time range")
            except KeyError:
                die("""When multiple time steps are present in a file, all of the 
                    time steps specified by the input arguments must exist in that file
                    """)
        else:
            ### Awkwardly handle scalar and length 1 coordinates
            log.debug("Dataset has one time point")
            if "time" in da.dims:
                da = da.isel(time=0)
            da = da.drop_vars("time")
    
    ### Check that what's left is sensible
    if "time" in da.coords:
        if len(da.coords) != 3:
            die(f"{field_name} has more than 2 non-time dimensions and we can't handle custom pressure levels yet")
    else:
        if len(da.coords) != 2:
            die(f"{field_name} has more than 2 non-time dimensions and we can't handle custom pressure levels yet")
    
    if not any([i in _lat_names for i in da.coords]):
        die(f"{field_name} does not have a latitude coordinate")
    ### Standardise latitude name
    for n in _lat_names[1:]:
        if n in da.coords:
            log.info(f"Renaming latitude coord: {n}")
            da = da.rename({n:"latitude"})

    if not any([i in _lon_names for i in da.coords]):
        die(f"{field_name} does not have a longitude coordinate")
    ### Standardise longitude name & shift to a 0-360 range if necessary
    for n in _lon_names[1:]:
        if n in da.coords:
            log.info(f"Renaming longitude coord: {n}")
            da = da.rename({n:"longitude"})
            if da.longitude[-1] < 180 and da.longitude.max().data > 180:
            # Roll longitude
                log.info(f"Longitude rolling required")
                da = da.roll(longitude=da.sizes["longitude"] // 2, roll_coords=True)
            log.debug(f"Reset lon to 0-360")
            da = da.assign_coords(longitude=(da.longitude + 360) % 360)
            da.sort_by(da.longitude)

    ### Does our custom field contain the whole domain we've requested?
    lat_buffer_range, lon_buffer_range = conf.get("domain_with_buffer")
    lat_range,        lon_range        = conf.get("domain")
    if ( conf.get("regrid_options") != "weight_file" ):
        da = da.sel(latitude=lat_buffer_range,longitude=lon_buffer_range)

    log.debug("Check domain is complete")
    if da.latitude.min().data > lat_range.stop or da.latitude.max().data < lat_range.start or da.longitude.min().data > lon_range.start or da.longitude.max().data < lon_range.stop:
        die(f"Domain of data: ({da.latitude.min().data},{da.longitude.min().data}) - ({da.latitude.max().data},{da.longitude.max().data}) does not fill the requested domain: ({lat_range.stop},{lon_range.start}) - ({lat_range.start},{lon_range.stop})")

    ### If everything checks out, add the source attribute
    da.attrs["source"] = file_name

    return da

def remaining_list(fields: Dict[Tuple[str,str],Era5field],dataset=None) -> list[str]:
    
    if dataset:
        field_list = [ i for (i,ds),field in fields.items() if ( ds == dataset and not field.is_complete() ) ]
    else:
        field_list = [ i for (i,_),field in fields.items() if not field.is_complete() ]
    ### Add in equivalents
    field_list = field_list + [ j for i,j in conf.get('equivalent_vars',{}).items() if i in field_list ]
    log.debug(f"Fields remaining: {field_list}")
    return field_list

def get_data(cats: list[Union[intake_esm.core.esm_datastore,NamedTuple]],t: Timestamp) -> Dict[Tuple[str,str],Era5field]:

    datasets = [ k for k in conf.get("fields").keys() ]
    inverse_equivs = { v:k for k,v in ( conf.get("equivalent_vars",{}) ).items() }
    lat_buffer_range, lon_buffer_range = conf.get("domain_with_buffer")
    static_fields = conf.get('static',{})
    custom_field_cat_key = conf.get("custom_field_catalogue_key")
    cat_names = [ i.name for i in cats ]

    ### CDO seems to want fields in a specific order
    ###   - single level
    ###   - pressure level
    ###   - static
    ### Do that here
    log.debug("initialise fields")
    fields=OrderedDict()
    for ds_type in datasets:
        log.debug(f"get dynamic field names with {ds_type}")
        for field_name in conf.get('fields')[ds_type]:
            static = static_fields.get(ds_type,[])
            log.debug(f"static fields for {ds_type}: {static}")
            if field_name not in static:
                log.debug(f"Initialise {field_name},{ds_type}")
                fields[(field_name,ds_type)] = Era5field(field_name)
    for ds_type in datasets:
        log.debug(f"get dynamic static names with {ds_type}")
        for field_name in static_fields.get(ds_type,[]):
            log.debug(f"Initialise static {field_name} {ds_type}")
            fields[(field_name,ds_type)] = Era5field(field_name)

    if "custom_fields" in conf:
        if custom_field_cat_key not in cat_names:
            log.info("Custom fields found, but no order specified, inserting at top")
            ### We have custom fields, but the user has not told
            ### us where they go, so they'll be processed first
            fakecat = namedtuple("FakeCatalogue","name")
            fakecat.name = custom_field_cat_key
            cats.insert(0,fakecat)

    for cat in cats:
        log.info(f"Searching for remaining fields in {cat}")
        if cat.name == custom_field_cat_key:
            log.info("Handling custom fields")
            for field, fn in conf.get('custom_fields').items():
                da = handle_custom_field(field,fn)
                realm = conf.get(f"custom_field_flags.{field}","global")
                log.debug(f"{field} from {fn} defined on {realm}")
                if realm == "subdomain":
                    raise(NotImplementedError("TODO"))
                ### Can only handle single-level custom fields
                fields[(field,'single-levels')].add_dataarray(da,realm)
        else:
            if "dataset" in cat.df:
                dataset = cat.df["dataset"].unique()[0]
                result = cat.search(parameter=remaining_list(fields,dataset),year=t.year,month=t.month)
            else:
                result = cat.search(parameter=remaining_list(fields),year=t.year,month=t.month)
            if len(result.df) == 0:
                log.debug("None Found")
                continue
            log.debug(f"Found: {result.df['file_variable']}")
            file_var_map=dict(zip(result.df['file_variable'],result.df['parameter']))
            chunks = conf.get(f'catalogue_flags.{cat.name}.chunks',"auto")
            log.debug("Creating dataset dict")
            if conf.get("data_types",32) == 32:
                ### Force 32-bit right from the start
                d = cat_to_dataset_dict(result,chunks)
            else:
                ### Don't care what it ends up as
                d = result.to_dataset_dict(xarray_open_kwargs={"chunks":chunks}, progressbar=False)
            for ds in d.values():
                for da in ds:
                    log.debug(f"Handling {da}")
                    ### Dataset realm
                    realm = conf.get(f'catalogue_flags.{cat.name}.realm',"global")
                    field_name= inverse_equivs.get(file_var_map[da],file_var_map[da])
                    if ( conf.get("regrid_options") == "weight_file" ):
                        log.debug("Not Trimming to buffered domain")
                        out_da = ds[da]
                    else:
                        log.debug("Trimming to buffered domain")
                        out_da = ds[da].sel(latitude=lat_buffer_range,longitude=lon_buffer_range)
                    out_da.attrs['source'] = cat.name
                    ### Field realm overrides dataset realm
                    if field_name in conf.get("ocean_only") or []:
                        realm = "ocean_only"
                    elif field_name in conf.get("land_only") or []:
                        realm = "land_only"
                    if "dataset" in cat.df:
                        fields[(field_name,dataset)].add_dataarray(out_da,realm)
                    else:
                        for (i,_),field in fields.items():
                            if i == field_name:
                                field.add_dataarray(out_da,realm)
                                break
    return fields

def load_fields(t: Timestamp) -> Dict[Tuple[str,str],Era5field]:

    cats = get_catalogues()
    return get_data(cats,t)

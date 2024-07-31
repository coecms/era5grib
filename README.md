# era5grib
Convert [NCI ERA5 archive](https://opus.nci.org.au/display/ERA5/ERA5+Community+Home) data to GRIB format

## Using era5grib

To use era5grib you will need to be a member of the [hh5](https://my.nci.org.au/mancini/project/hh5/join) (conda), [rt52](https://my.nci.org.au/mancini/project/rt52/join) (ERA5) and [zz93](https://my.nci.org.au/mancini/project/zz93/join) (ERA5-land) projects at NCI

Load the conda environment with
```
module use /g/data/hh5/public/modules
module load conda/analysis3
```

Then run the application. 

### Common use cases

The most common use cases are creating initial conditions for either the Unified Model or WRF directly from ERA5 and ERA5Land data. For example:
```
era5grib -f um_era5land --time 20200101T1200 --target qrparm.mask --output out.grib
```
will create a GRIB file named `out.grib` for a Unified Model LAM configuration that can be passed to the reconfiguration program to create initial conditions for a model run commencing at 1200 UTC on 1st January 2020 using ERA5-Land data supplemented by ERA5 data over ocean points; or
```
era5grib -f wrf_era5land --namelist namelist.wps --geo geo_em.d01.nc --output GRIBFILE.AAA
```
will create a GRIB file named `GRIBFILE.AAA` using the start time specification from `namelist.wps` and the domain from `geo_em.d01.nc` for use in generating WRF initial conditions using ERA5-Land data supplemented by ERA5 data over ocean points. The `-f` option specifies a YAML configuration file, and can either be the path to a file or the key for one of the four known configurations:
* `wrf_era5land` - Creates a GRIB1 file with all fields necessary for WPS initial conditions from regridded ERA5Land + ERA5 data. 
* `wrf_era5` - Creates a GRIB1 file with all fields necessary for WPS initial conditions from only ERA5 data.
* `um_era5land` - Creates a GRIB1 file with all fields necessary for UM reconfiguration initial conditions from regridded ERA5Land + ERA5 data. 
* `um_era5` - Creates a GRIB1 file with all fields necessary for UM reconfiguration initial conditions from only ERA5 data.

The YAML files corresponding to these known configurations can be found in the `config` subdirectory of the package. 

### Legacy run options

In order to maintain backwards compatibility with earlier versions of `era5grib`, legacy run modes where the 'model' keyword (`wrf` or `um`) is the second argument are supported. 

```
era5grib wrf --namelist namelist.wps --geo geo_em.d01.nc --output GRIBFILE.AAA
```
or
```
era5grib um --time 20200101T1200 --target qrparm.mask --output era5.20200101T1200.grib
```
The `wrf` and `um` options are equivalent to passing `-f wrf_era5land` and `-f um_era5land` respectively. The `--no-era5land` flag is respected only in this variation, and will change the configuration files to `-f wrf_era5` and `-f um_era5` respectively. `--no-era5land` is ignored if the `-f` option is specified.

### Advanced use cases.



## Command line reference

`era5grib` takes the following command line options


## Configuration reference

The following is a reference for the configuration parameters of `era5grib`. Where a 'Default' is listed, this refers to the contents of `config/default.yaml`.

### Output specification

`fields.<dataset>` *List[str]*:  
List of fields from `<dataset>` to extract from each of the named intake catalogues and/or custom field files. `<dataset>` must correspond to an index in one or more of the specified intake catalogues. All fields must be present in their respective datasets, or the application will exit with an error. For ERA5 and ERA5Land, valid datasets are `single-levels` and `pressure-levels`. If `fields` is not provided, the application will exit with an error.

`custom_fields` *Dict[str,str]*:  
A mapping of fields to include external to the specified catalogues where the key is the name of the field and the value is the file in which the field will be found. Only netCDF is accepted for custom fields, and the field name within the file must correspond to the key used. The field in the file must have `latitude` and `longitude` coordinates (or equivalent) and must contain the entire domain as defined by the WRF geogrid file or UM mask file. The field must contain a single time point, or, for output containing multiple time steps, it must contain every time step expected by the output file. Used for customising initial conditions by e.g. inserting climatologies. 

`custom_field_flags` *Dict[str,str]*:  
Defines the realm that each custom field applies to. Valid values are `global`, `land_only` and `ocean_only`.

`land_only` *List[str]*:  
A list of fields in all catalogues that are only defined over land. Fields in this list are assumed to be defined on a single level. Used to determine if a field read on a realm other than `global` is fully defined:
* Default: `[ stl1, stl2, stl3, stl4, swvl1, swvl2, swvl3, swvl4 ]`

`ocean_only` *List[str]*:  
A list of fields in all catalogues that are only defined over the ocean. Fields in this list are assumed to be defined on a single level. Used to determine if a field read on a realm other than `global` is fully defined:  
* Default: `[ ci, msl, sst ]`

`static.<dataset>` *List[str]*:  
A list of fields in all catalogues that are static in time. These fields are handled differently when combining a dataset with more than one time value. 
Defaults:
* None
* `static.single-levels: [ lsm, z ]`

`equivalent_vars` *Dict[str,str]*:  
A mapping of fields names that are treated as equivalent.
* Default: `{ 10u: u10, 10v: v10 }`

`format` *str*:  
Final output format. Allowed values are `grib` for GRIB1 format and `netcdf`.
* Default: `grib`

`data_types` *int*:  
Size in bytes of floating point output data types.
* Default: `32`

### Intake catalogues

`catalogue_path` *List[str]*:  
Path to the `yaml` files that contain intake catalogue data. The paths will be searched in the order they are listed.  
* Default: `[ /g/data/hh5/public/apps/nci-intake-catalogue/catalogue_new.yaml, ]`

`catalogues` *List[str]*:  
Names of catalogues to search through when looking for fields. The catalogues will be searched in the order they are listed. A special name, defined by the `custom_field_catalogue_key` configuration value, determines the order of custom fields relative to catalogues.   
* Default: `[ era5land, era5 ]`

`catalogue_flags.<catalogue>`:  
Configuration applied to the catalogue `<catalogue>`. These are optional, defaults will be provided if any flags are not specified. A dictionary can be provided here, but the preferred method of configuring `catalogue_flags` is through namespaces separated by `.` characters. Individual options described below:

`catalogue_flags.<catalogue>.realm` *str*:  
Define the domain over which the data in this catalogue is defined. Valid values are `global`, `land_only` or `ocean_only`.  
Defaults: 
* `global`
* `catalogue_flags.era5land.realm: land_only`

`catalogue_flags.<catalogue>.product_type` *str*:  
The `product_type` to filter on.  
* Default: `'reanalysis'`

`catalogue_flags.<catalogue>.sub_collection_pref` *str*: The subcollection of `<catalogue>` to search for fields before searching through the main collection.
Defaults: 
* `None`
* `catalogue_flags.era5.sub_collection_pref: era5-1`

`catalogue_flags.<catalogue>.chunks` *Dict[str,int]*:  
Chunk specification for data from `<catalogue>` loaded in xarray.  
Defaults:
* `catalogue_flags.era5land.chunks: {time: 1, level: 37, latitude: -1, longitude: -1}`
* `catalogue_flags.era5.chunks: {time: 12, level: 5, latitude: -1, longitude: -1}`

`land-mask.<catalogue>` *str*:  
The name of the land mask field in `<catalogue>` - used in certain cases for combining fields over multiple realms.  
* Default: `land-mask.era5: lsm`

`metadata_catalogue` *str*:  
Name of catalogue from which to draw GRIB parameters of the fields to be loaded.  
* Default: `ecmwf.grib_parameters`

`metadata_mapping` *Dict[str,str]*:  
A map of keys in the metadata catalogue to map to attributes of fields in the final dataset. Metadata keys not listed in this option will not be present in the final dataset.  
* Default: `{ table: table2Version, code: indicatorOfParameter, standard_name: cfName, ecmwf_name: name, ecmwf_shortname: shortName, units: units }`

`custom_field_catalogue_key` *str*:  
A placeholder value to denote the order that custom fields will be processed with respect to intake catalogues:
* Default: `custom_fields`

### Regridding and coordinates

`regrid` *str*:  
If multiple grid sizes are detected in the input, this value of this key determines the target grid specification for the final output. Valid values are any of the supplied catalogue names, or the paths to any custom field files. If set to `None`, no regridding will be attempted and the application will exit with an error if multiple grid sizes are detected.  
* Default: `era5`

`regridding` *Dict[str,str]*:  
Reference parameters for regridding. Used to load a reference field in the event that a field meeting the regridding target specification has not been already loaded.
* Default: `{ ref_field: 2t, ref_date: '20010101' }`

`regrid_options` *str*:  
Select from the two available methods for handling NaN's in `land_only` or `ocean_only` fields when being merged onto global fields. Valid values are:
* `weight_file` - Use a pre-determined weight file that fills ocean points with extrapolated land points. Only available when regridding from `era5land` to `era5`. Bitwise-reproducible with legacy era5grib application.
* `interpolating` - Use the following method of applying `scipy` `interpolate_na` function for missing values:
```
- If domain corners are NaN, fill with average value of field
- lat_first = field.interpolate_na(dim=latitude).interpolate_na(dim=longitude)
- lon_first = field.interpolate_na(dim=longitude).interpolate_na(dim=latitude)
- return (lat_first + lon_first / 2)
```
* Default: `weight_file`

`polar` *bool*:  
Shifted pole for domains close to north/south pole.
* Default: `False`

### Application internal configuration

`includes` *str*:  
Either a path to another era5grib configuration file, or the tag of one of the four provided configurations that will be included when reading the current configuration file. Values specified in a configuration in an `include` statement will be overwritten by those from the current file.

`dataset_tags` *Dict[str,str]*:  
Internal tags used to track different types of dataset.  
* Default: `{ single-levels: surf, pressule-levels: pl }`

`log_level` *str* or *int*:  
Application logging level as specified by the [Python logging How-To guide](https://docs.python.org/3/howto/logging.html). 
* Default: `warning`


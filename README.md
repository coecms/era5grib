# era5grib
Convert [NCI ERA5 archive](https://opus.nci.org.au/display/ERA5/ERA5+Community+Home) data to GRIB format

## Using era5grib

To use era5grib you will need to be a member of the [hh5](https://my.nci.org.au/mancini/project/hh5/join) (conda), [rt52](https://my.nci.org.au/mancini/project/rt52/join) (ERA5) and [zz93](https://my.nci.org.au/mancini/project/zz93/join) (ERA5-Land) projects at NCI

Load the conda environment with
```
module use /g/data/hh5/public/modules
module load conda/analysis3
```

Then run the application. 

### Common use cases

The most common use cases are creating initial conditions for either the Unified Model or WRF directly from ERA5 and ERA5-Land data. For example:
```
era5grib -f um_era5land --time 20200101T1200 --target qrparm.mask --output out.grib
```
will create a GRIB file named `out.grib` for a Unified Model LAM configuration that can be passed to the reconfiguration program to create initial conditions for a model run commencing at 1200 UTC on 1st January 2020 using ERA5-Land data supplemented by ERA5 data over ocean points; or
```
era5grib -f wrf_era5land --namelist namelist.wps --geo geo_em.d01.nc --output GRIBFILE.AAA
```
will create a GRIB file named `GRIBFILE.AAA` using the start time specification from `namelist.wps` and the domain from `geo_em.d01.nc` for use in generating WRF initial conditions using ERA5-Land data supplemented by ERA5 data over ocean points. The `-f` option specifies a YAML configuration file, and can either be the path to a file or the key for one of the four known configurations:
* `wrf_era5land` - Creates a GRIB1 file with all fields necessary for WPS initial conditions from regridded ERA5-Land + ERA5 data. 
* `wrf_era5` - Creates a GRIB1 file with all fields necessary for WPS initial conditions from only ERA5 data.
* `um_era5land` - Creates a GRIB1 file with all fields necessary for UM reconfiguration initial conditions from regridded ERA5-Land + ERA5 data. 
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

Most of the time, simply converting netCDF ERA5/ERA5-Land data will be sufficient for constructing initial conditions for UM and WRF experiments. However, `era5grib` permits custom experiments without having to manually manipulate GRIB output files. All necessary configuration can be set in the configuration file. For the following examples, the configuration file will be named `custom.yaml`, and `era5grib` can be invoked as:
```
era5grib -f custom.yaml --time 20200101T1200 --target qrparm.mask --output out.grib
```
for the UM, or 
```
era5grib -f custom.yaml --namelist namelist.wps --geo geo_em.d01.nc --output GRIBFILE.AAA
```
for WRF. For many cases, a small modification to a known configuration will be required. In this case, the `includes` key is generally placed first in a custom configuration file. The following `custom.yaml` is a valid configuration:
```
includes: wrf_era5
```
And is exactly equivalent to running `era5grib -f wrf_era5 ...`. Any further options in `custom.yaml` will override options set by a configuration specified in `includes`. Running `era5grib` with the `--debug` flag or including `log_level: info` or `log_level: debug` in a custom configuration will have it print its working configuration.

#### Example - Replace an ERA5 field from a WRF run with a new field.
In this case, a custom skin temperature field has been generated, and we wish to replace the ERA5 skin temperature field with this new custom field. `custom.yaml` will be as follows:
```
includes: wrf_era5
custom_fields:
  skt: /path/to/custom/skt.nc
```
The `includes` key tells the configuration to inherit all settings from the `wrf_era5` known configuration, and the custom field will take preference over the `skt` field provided by ERA5. The custom field must contain either one time stamp, the timestamp specified by the `--time` or `--start` argument, or hourly timestamps spanning the times specified in the `--start` and `--end` arguments. It must also have the same name as the field(s) it is replacing, and must have `latitude` and `longitude` dimensions (though variatons of those are acceptable e.g. `LAT`, `LATITUDE`, etc.)

In this case, the custom field must be derived from ERA5 data or match the ERA5 grib. In order to accept a custom field on any grid, so long as it contains the full geographical region specified the file passed in the `--target` or `--geo` arguments, `custom.yaml` must also provide a `regrid` key:
```
includes: wrf_era5
custom_fields:
  skt: /path/to/custom/skt.nc
regrid: era5
```
#### Example - Replace an ERA5 field, then replace the land values of that field with ERA5-Land data for a UM run
In this case, we're still using a custom skin temperature field, however, we only want it to persist over the ocean, and we want to use ERA5-Land data over the land. In this case, we want the custom field to take precedence over the fields from ERA5, however, we want the ERA5-Land fields to take precedence over the custom fields. To do this, we use the `catalogues` key which can modify the order in which the various data sources are handled.
```
includes: um_era5land
custom_fields:
  skt: /g/data/v45/cc6171/3JuneERA5_SKT_with_box_and_buffer.nc
catalogues:
  - era5_land
  - custom_fields
  - era5
```
Note the `custom_fields` list entry under `catalogues`. This is a special 'catalogue' (defined by the `custom_field_catalogue_key` configuration key, `custom_fields` by default) that instructs `era5grib` to process custom fields. Data from each of the data sources is handled in the order of the `catalogues` list, so data from ERA5-Land will be processed first, then the custom fields, then ERA5 last. Processing stops when a domain is considered complete, so for land-only fields (e.g. soil temperatures), only ERA5-Land data is considered. For global fields, however, processing will continue down the list of catalogues. Therefore, the skin temperature field will be considered complete after the custom field has been loaded. The remainder of the global or ocean-only fields will then be satisfied by the ERA5 dataset.

#### Example - Selecting arbitrary fields
In this case, we are extracting a series of ERA5 fields for neither the UM nor WRF, meaning that an entirely different set of fields needs to be specified. The minimum specification in this case is the `fields` key, which must contain `single-levels` and `pressure-levels` keys.
```
fields:
  single-levels:
    - 10u
    - 10v
    - sst
    - lsm
    - z
  pressure-levels:
    - u
    - v
    - z
```
This is necessary as the field `z` appears in both single level and pressure level ERA5 data with different meanings. In this configuration, both ERA5 and ERA5-Land datasets will be used, to use only ERA5, add the `catalogues` key to the configuration with:
```
fields:
  single-levels:
    - 10u
    - 10v
    - sst
    - lsm
    - z
  pressure-levels:
    - u
    - v
    - z
catalogues:
  - era5
```
Finally, `era5grib` can be used as a custom ERA5 field accumulator by setting the output format to `netcdf` instead of GRIB:
```
fields:
  single-levels:
    - 10u
    - 10v
    - sst
    - lsm
    - z
  pressure-levels:
    - u
    - v
    - z
catalogues:
  - era5
format: netcdf
```

## Command line reference

`era5grib` takes the following command line options

**\[model]**  
&nbsp;&nbsp;&nbsp;&nbsp;Legacy entry point. Must be one of `um` or `wrf` and must be the first argument to `era5grib`

**-f**, **--file**[=]NAME  
&nbsp;&nbsp;&nbsp;&nbsp;YAML configuration file

**-o**, **--output**[=]NAME  
&nbsp;&nbsp;&nbsp;&nbsp;Name of the output GRIB/netCDF file

**--time**[=]ISOTIME  
&nbsp;&nbsp;&nbsp;&nbsp;Timestamp for output file

**--namelist**[=]NAME  
&nbsp;&nbsp;&nbsp;&nbsp;Read start and end times from WPS namelist. Ignored if **--time** is specified

**--start**[=]ISOTIME  
&nbsp;&nbsp;&nbsp;&nbsp;Start time for multi-time output file. Ignored if **--namelist** or **--time** is specified

**--end**[=]ISOTIME  
&nbsp;&nbsp;&nbsp;&nbsp;End time for multi-time output. Ignored if **--namelist** is specified

**--geo**[=]NAME  
&nbsp;&nbsp;&nbsp;&nbsp;Geogrid file for trimming domain for WPS ungrib input. Ignored if **\[model]** is `um`

**--target**[=]NAME  
&nbsp;&nbsp;&nbsp;&nbsp;UM file on target grid for trimming domain for UM reconfiguration. Ignored if **\[model]** is `wrf`

**--format**[=]FORMAT  
&nbsp;&nbsp;&nbsp;&nbsp;Output file format. Must be one of `grib` or `netcdf`. This argument takes precedence over the format specified in the configuration file.

**--debug**  
&nbsp;&nbsp;&nbsp;&nbsp;Set the log level to `debug`. This argument takes precedence over the log level specified in the configuration files.

**--\[no]era5land**  
&nbsp;&nbsp;&nbsp;&nbsp;Enable/disable ERA5-Land catalogue. Ignored if **-f** is specified.

**--\[no]polar**  
&nbsp;&nbsp;&nbsp;&nbsp;Include all longitudes. Ignored if **-f** is specified.


## Configuration reference

The following is a reference for the configuration parameters of `era5grib`. Where a default is listed, this refers to the contents of `config/default.yaml`. Where a default is not listed, the parameter is optional.

### Output specification

`fields.<dataset>` *List[str]*:  
List of fields from `<dataset>` to extract from each of the named intake catalogues and/or custom field files. `<dataset>` must correspond to an index in one or more of the specified intake catalogues. All fields must be present in their respective datasets, or the application will exit with an error. For ERA5 and ERA5-Land, valid datasets are `single-levels` and `pressure-levels`. If `fields` is not provided, the application will exit with an error.

`custom_fields` *Dict[str,str]*:  
A mapping of fields to include external to the specified catalogues where the key is the name of the field and the value is the file in which the field will be found. Only netCDF is accepted for custom fields, and the field name within the file must correspond to the key used. The field in the file must have `latitude` and `longitude` coordinates (or equivalent) and must contain the entire domain as defined by the WRF geogrid file or UM mask file. The field must contain a single time point, or, for output containing multiple time steps, it must contain every time step expected by the output file. Used for customising initial conditions by e.g. inserting climatologies. 

`custom_field_flags` *Dict[str,str]*:  
Defines the realm that each custom field applies to. Valid values are `global`, `land_only` and `ocean_only`.

`land_only` *List[str]*:  
A list of fields in all catalogues that are only defined over land. Fields in this list are assumed to be defined on a single level. Used to determine if a field read on a realm other than `global` is fully defined:
&nbsp;&nbsp;&nbsp;&nbsp; Default: `[ stl1, stl2, stl3, stl4, swvl1, swvl2, swvl3, swvl4 ]`

`ocean_only` *List[str]*:  
A list of fields in all catalogues that are only defined over the ocean. Fields in this list are assumed to be defined on a single level. Used to determine if a field read on a realm other than `global` is fully defined:  
&nbsp;&nbsp;&nbsp;&nbsp; Default: `[ ci, msl, sst ]`

`static.<dataset>` *List[str]*:  
A list of fields in all catalogues that are static in time. These fields are handled differently when combining a dataset with more than one time value. 
Defaults:
* `None`
* `static.single-levels: [ lsm, z ]`

`equivalent_vars` *Dict[str,str]*:  
A mapping of fields names that are treated as equivalent.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `{ 10u: u10, 10v: v10 }`

`format` *str*:  
Final output format. Allowed values are `grib` for GRIB1 format and `netcdf`.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `grib`

`data_types` *int*:  
Size in bytes of floating point output data types.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `32`

### Intake catalogues

`catalogue_path` *List[str]*:  
Path to the `yaml` files that contain intake catalogue data. The paths will be searched in the order they are listed.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `[ /g/data/hh5/public/apps/nci-intake-catalogue/catalogue_new.yaml, ]`

`catalogues` *List[str]*:  
Names of catalogues to search through when looking for fields. The catalogues will be searched in the order they are listed. A special name, defined by the `custom_field_catalogue_key` configuration value, determines the order of custom fields relative to catalogues.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `[ era5land, era5 ]`

`catalogue_flags.<catalogue>`:  
Configuration applied to the catalogue `<catalogue>`. These are optional, defaults will be provided if any flags are not specified. A dictionary can be provided here, but the preferred method of configuring `catalogue_flags` is through namespaces separated by `.` characters. Individual options described below:

`catalogue_flags.<catalogue>.realm` *str*:  
Define the domain over which the data in this catalogue is defined. Valid values are `global`, `land_only` or `ocean_only`.  
Defaults: 
* `global`
* `catalogue_flags.era5land.realm: land_only`

`catalogue_flags.<catalogue>.product_type` *str*:  
The `product_type` to filter on.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `'reanalysis'`

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
&nbsp;&nbsp;&nbsp;&nbsp;Default: `land-mask.era5: lsm`

`metadata_catalogue` *str*:  
Name of catalogue from which to draw GRIB parameters of the fields to be loaded.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `ecmwf.grib_parameters`

`metadata_mapping` *Dict[str,str]*:  
A map of keys in the metadata catalogue to map to attributes of fields in the final dataset. Metadata keys not listed in this option will not be present in the final dataset.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `{ table: table2Version, code: indicatorOfParameter, standard_name: cfName, ecmwf_name: name, ecmwf_shortname: shortName, units: units }`

`custom_field_catalogue_key` *str*:  
A placeholder value to denote the order that custom fields will be processed with respect to intake catalogues:  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `custom_fields`

### Regridding and coordinates

`regrid` *str*:  
If multiple grid sizes are detected in the input, this value of this key determines the target grid specification for the final output. Valid values are any of the supplied catalogue names, or the paths to any custom field files. If not present or set to `None`, no regridding will be attempted and the application will exit with an error if multiple grid sizes are detected.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `era5`

`regridding` *Dict[str,str]*:  
Reference parameters for regridding. Used to load a reference field in the event that a field meeting the regridding target specification has not been already loaded.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `{ ref_field: 2t, ref_date: '20010101' }`

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
&nbsp;&nbsp;&nbsp;&nbsp;Default: `weight_file`

`polar` *bool*:  
Include all longitudes.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `False`

### Application internal configuration

`includes` *str*:  
Either a path to another era5grib configuration file, or the tag of one of the four provided configurations that will be included when reading the current configuration file. Values specified in a configuration in an `include` statement will be overwritten by those from the current file.

`dataset_tags` *Dict[str,str]*:  
Internal tags used to track different types of dataset.  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `{ single-levels: surf, pressule-levels: pl }`

`log_level` *str* or *int*:  
Application logging level as specified by the [Python logging How-To guide](https://docs.python.org/3/howto/logging.html).  
&nbsp;&nbsp;&nbsp;&nbsp;Default: `warning`


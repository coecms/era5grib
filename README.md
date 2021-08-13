# era5grib
Convert [NCI ERA5 archive](https://opus.nci.org.au/display/ERA5/ERA5+Community+Home) data to GRIB format

## Using era5grib

To use era5grib you will need to be a member of the [hh5](https://my.nci.org.au/mancini/project/hh5/join) (conda), [rt52](https://my.nci.org.au/mancini/project/rt52/join) (ERA5) and [zz93](https://my.nci.org.au/mancini/project/zz93/join) (ERA5-land) projects at NCI

Load the conda environment with
```bash
module use /g/data3/hh5/public/modules
module load conda/analysis3
```

## Values over land

Variables over land use ERA5land instead of ERA5 surface values, as ERA5land is deemed to be more accurate.
ERA5land values are regridded to the ERA5 grid using a bilinear interpolation method, with data over the ocean filled in using ERA5 surface values where available.

Between 1979-01-01 and the ERA5land start date 1981-01-01 values come purely from ERA5. Additionally some ERA5land fields are only present from 1981-01-01T01:00 onwards,
in which case the land values from 01:00 are used for 1981-01-01T00:00

Using ERA5land can be disabled with the option `--no-era5land`

### WRF output

era5grib can read in the model run dates from your WPS namelist, or you can specify the start and end dates on the command line.
You can also pass in your geogrid output file and only that area will be converted to GRIB format.

```bash
era5grib wrf --namelist namelist.wps --geo geo_em.d01.nc --output GRIBFILE.AAA
```

The output can be passed to ungrib, using the ERA-Interim pressure level Vtable

### UM output

The UM can only process one date at a time, which should be specified on the command line.
The area to convert can be specified using any UM file on the target grid, e.g. the land mask.

```bash
era5grib um --time 20200101T1200 --target qrparm.mask --output era5.20200101T1200.grib
```

The output file should then be processed using the UM reconfiguration


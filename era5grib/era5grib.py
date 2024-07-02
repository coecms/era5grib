#!/g/data/hh5/public/apps/nci_scripts/python-analysis3
# Copyright 2020 Scott Wales
# author: Scott Wales <scott.wales@unimelb.edu.au>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path
from .main import main

from pandas import Timestamp

def era5grib_wrf(
    start=None,
    end=None,
    output=None,
    namelist=None,
    geo=None,
    source="NCI",
    format="grib",
    era5land: bool = True,
    polar: bool = False,
):
    """
    Legacy entry point preserved for compatibility

    Convert the NCI ERA5 archive data to GRIB format for use in WRF limited
    area modelling.

    Will generate a grib file for a time range (including the fixed z and lsm
    fields), to be processed by WPS's ungrib tool. Dates can be read from a WPS
    namelist or by supplying a start and end time.

    The output area can be limited to reduce file size by supplying the geogrid
    output file as 'geo'.
    """
    if era5land:
        conf_file = str(Path(__file__).parent.parent / 'config' / 'wrf_era5land.yaml')
    else:
        conf_file = str(Path(__file__).parent.parent / 'config' / 'wrf_era5.yaml')

    if polar:
        polar_flag="--polar"
    else:
        polar_flag="--no-polar"

    if isinstance(time,Timestamp):
        time = time.strftime("%Y%m%d%H%M")

    if isinstance(output,Path):
        output = str(output)

    main(["--file",conf_file,"--start",start,"--end",end,"--output",output,"--namelist",namelist,"--geo",geo,"--format",format,polar_flag])


def era5grib_um(
    time,
    output=None,
    target=None,
    source="NCI",
    format="grib",
    era5land: bool = True,
    polar: bool = False,
):
    """
    Legacy entry point preserved for compatibility

    Convert the NCI ERA5 archive data to GRIB format for use in UM limited area
    modelling.

    Will generate a grib file for a single time, to be processed by the UM
    reconfiguration.

    The output area can be limited to reduce file size by supplying a UM file
    on the target grid as 'target'
    """

    if era5land:
        conf_file = str(Path(__file__).parent / 'config' / 'um_era5land.yaml')
    else:
        conf_file = str(Path(__file__).parent / 'config' / 'um_era5.yaml')

    if polar:
        polar_flag="--polar"
    else:
        polar_flag="--no-polar"

    if isinstance(time,Timestamp):
        time = time.strftime("%Y%m%d%H%M")

    if isinstance(output,Path):
        output = str(output)

    main(["--file",conf_file,"--time",time,"--output",output,"--target",target,"--format",format,polar_flag])


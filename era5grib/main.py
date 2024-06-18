"""
Convert the NCI ERA5 archive data to GRIB format for use in limited area modelling.
Can output the required fields for both WRF and UM models or any other combination of
ERA5 and ERA5Land fields. Can read from custom or standard catalogues and can accept
replacement files for individual fields
"""
from .data_handling import data_read, data_combine
from . import command_line
from .config import conf
from .parallel import start_parallel, stop_parallel

import sys
from typing import Optional, List
    
def main(in_args: Optional[List[str]] = None):
    if in_args is None:
        in_args = sys.argv[1:]

    start_parallel()

    command_line.parse_args(in_args)
    fields={}
    for t in conf.get_month_range():
        fields[t]=data_read.load_fields(t)
    ds = data_combine.combine(fields)
    conf.get("writer")(ds)

    stop_parallel()

if __name__ == "__main__":
    main()
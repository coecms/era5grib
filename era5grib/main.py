"""
Convert the NCI ERA5 archive data to GRIB format for use in limited area modelling.
Can output the required fields for both WRF and UM models or any other combination of
ERA5 and ERA5Land fields. Can read from custom or standard catalogues and can accept
replacement files for individual fields
"""
from . import command_line
import sys
from typing import Optional, List
    
def main(in_args: Optional[List[str]] = None):
    if in_args is None:
        in_args = sys.argv[1:]

    command_line.parse_args(in_args)
    fields = data_read.load_fields()
    data_write.write(fields)

if __name__ == "__main__":
    main()
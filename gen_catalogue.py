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

import xarray
import pandas
import json
from glob import glob
from pathlib import Path

vrows = []

with open("era5_vars.json") as f:
    code_dict = json.load(f)


def find_code(var):
    for k, (name, long_name) in code_dict.items():
        if var == name:
            return k.split(".")
    return None, None


for product in ["land", "surface", "pressure"]:
    for p in (Path(f"/g/data/ub4/era5/netcdf") / product).iterdir():
        try:
            print(p)
            da = xarray.open_dataarray(str(next(p.glob("2019/*_20191101_*.nc"))))

            code, table = find_code(da.name)

            vrows.append([product, p, da.name, da.attrs["long_name"], code, table])
        except StopIteration:
            # Not hourly data
            pass

df = pandas.DataFrame(
    vrows, columns=["product", "dirname", "varname", "long_name", "code", "table"]
)
df.to_csv("index.csv", index=False)
print(df)

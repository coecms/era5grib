#!/g/data/hh5/public/apps/nci_scripts/python-analysis3
# Copyright 2021 Scott Wales
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

from era5grib.nci import *
import pandas
import pytest


def test_19810101T0000():
    # era5land only available for some fields
    date = pandas.to_datetime("19810101T0000")
    ds = read_wrf(date, date)

    assert numpy.all(numpy.isfinite(ds.sp_surf))


def test_19810101T0100():
    # era5land available for all fields
    date = pandas.to_datetime("19810101T0100")
    ds = read_wrf(date, date)

    assert numpy.all(numpy.isfinite(ds.sp_surf))


def test_19790101T0000():
    # era5land not available
    date = pandas.to_datetime("19790101T0000")
    ds = read_wrf(date, date)

    assert numpy.all(numpy.isfinite(ds.sp_surf))


def test_19781231T2300():
    # era5 not available
    date = pandas.to_datetime("19781231T2300")

    with pytest.raises(ValueError):
        ds = read_wrf(date, date)

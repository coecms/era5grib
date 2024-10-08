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

__all__ = [
    "main",
    "era5grib",
    "data_handling",
    "config",
    "domain",
    "logging",
    "parallel",
    "command_line",
    "__version__"
    ]

from .era5grib import (main,
                       era5grib,
                       data_handling,
                       config,
                       domain,
                       logging,
                       parallel,
                       command_line)

from ._version import __version__

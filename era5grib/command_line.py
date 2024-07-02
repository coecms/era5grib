import argparse
import textwrap
import pandas
import f90nml
import importlib

from pathlib import Path

from .config import conf
from .logging import log, die
from . import domain
from logging import DEBUG
from .main import __doc__ as maindoc

from typing import Optional, List

def handle_args(
        model: Optional[str]=None,
        file: Optional[str]=None,
        output: Optional[str]=None,
        namelist: Optional[str]=None,
        geo: Optional[str]=None,
        target: Optional[str]=None,
        time: Optional[str]=None,
        start: Optional[str]=None,
        end: Optional[str]=None,
        format: str="grib",
        era5land: bool = True,
        polar: Optional[bool] = None,
        debug: Optional[bool] = False,
    ) -> None:
    
    ### Cmdline > local conf > default conf
    conf_path = Path(__file__).parent / 'config'

    ###Convert times to what we need first
    if time:
        time = pandas.to_datetime(time).floor('h')
    if start:
        start = pandas.to_datetime(start).floor('h')
    if end:
        end = pandas.to_datetime(end).ceil('h')

    ### Handle 'wrf' model
    if model == "wrf":
        if era5land:
            conf.update(conf_path / 'wrf_era5land.yaml')
        else:
            conf.update(conf_path / 'wrf_era5.yaml')
        
        if debug:
            conf.set('log_level',DEBUG)
        log.start(conf.get('log_level'))

        if output is None:
            log.info("Output file not provided - using default")
            output = "GRIBFILE.AAA"

        if namelist:
            with open(namelist,'r') as f:
                nml = f90nml.read(f)
                if start is None:
                    start=pandas.to_datetime(
                    nml["share"]["start_date"], format="%Y-%m-%d_%H:%M:%S"
                )
                    try:
                        start = start.min()
                    except:
                        pass
                if end is None:
                    end = pandas.to_datetime(
                        nml["share"]["end_date"], format="%Y-%m-%d_%H:%M:%S"
                    )
                    try:
                        end = end.max()
                    except:
                        pass
        
        if start is None:
            die("Please provide either 'start' or 'namelist' if 'wrf' is selected as a model")

        if geo is not None:
            conf.set('domain',domain.get_domain(Path(geo),polar))
        else:
            log.warning("Outputting the full domain, use --geo=geo_em.d01.nc to limit")
            conf.set('domain',domain.get_domain(None,polar))

    elif model == "um":

        if era5land:
            conf.update(conf_path / 'um_era5land.yaml')
        else:
            conf.update(conf_path / 'um_era5.yaml')

        if debug:
            conf.set('log_level',DEBUG)
        log.start(conf.get('log_level'))

        if output is None:
            output = f"um.era5.{time.strftime('%Y%m%dT%H%M')}.grib"
        
        if time:
            start = time

        if target is not None:
            conf.set('domain',domain.get_domain(Path(target),polar))
        else:
            log.warning("Outputting the full domain, use --target=qrparm.mask to limit")
            conf.set('domain',domain.get_domain(None,polar))

    elif model is None:

        if file is None:
            die("If 'um' or 'wrf' is not specified, a conf file must be provided")
        conf.update(file)

        if debug:
            conf.set('log_level',DEBUG)
        log.start(conf.get("log_level"))

        if target is not None:
            conf.set('domain',domain.get_domain(Path(target),polar))
        elif geo is not None:
            conf.set('domain',domain.get_domain(Path(geo),polar))
        else:
            conf.set('domain',domain.get_domain(None,polar))

        if time:
            start = time
        elif namelist:
            with open(namelist,'r') as f:
                nml = f90nml.read(f)
                if start is None:
                    start=pandas.to_datetime(
                    nml["share"]["start_date"], format="%Y-%m-%d_%H:%M:%S"
                )
                    try:
                        start = start.min()
                    except:
                        pass
                if end is None:
                    end = pandas.to_datetime(
                        nml["share"]["end_date"], format="%Y-%m-%d_%H:%M:%S"
                    )
                    try:
                        end = end.max()
                    except:
                        pass

    if start is None:
        die("Either 'time', 'start' or 'namelist' must be provided in order to construct time bounds")

    if end is None:
        end = start
        
    fmt = conf.get("format",format)
    if output is None:
        log.warning(f"Output file name not specified, using out.{fmt}")
        output = 'out.' + fmt
    try:
        writer=importlib.import_module(f"era5grib.output_drivers.{fmt}").write
    except ModuleNotFoundError:
        die("Error! Invalid format specifier. A format must have a corresponding python source file in output_drivers and contain a 'write' function")
    except AttributeError:
        die(f"Error! Output driver for {format} does not have a 'write' function")

    ### Derived config
    conf.set('writer',writer)
    conf.set('start',start)
    conf.set('end',end)
    conf.set('output',output)
    conf.set('domain_with_buffer',domain.get_domain_with_buffer(*conf.get("domain")))

    if polar is not None:
        conf.set("polar",polar)
    elif conf.get("polar",None) is None:
        conf.set("polar",False)

    log.info(conf.get("default_config"))
    log.info(conf.get("model_config"))

    if not conf.get("fields"):
        die("No fields specified!")

    if conf.get("regrid") != 'era5' and conf.get("regrid_options") == "weight_file":
        die("ERROR: Weight file regridding option only supports regridding to 'era5'")

def parse_args(in_args: List[str]) -> None:
    f = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(
        description=textwrap.dedent(maindoc), formatter_class=f
    )
    parser.add_argument('model',nargs='?',help="Legacy entry points, must be one of 'wrf' or 'um'")
    parser.add_argument('-f','--file',help="YAML configuration file",type=Path)
    parser.add_argument('-o','--output',help="Output file",required=True,type=Path)
    parser.add_argument("--namelist", help="Read start and end dates from WPS namelist",type=Path)
    parser.add_argument("--time", help="Output time", type=pandas.to_datetime)
    parser.add_argument("--start", help="Output start time", type=pandas.to_datetime)
    parser.add_argument("--end", help="Output end time", type=pandas.to_datetime)
    parser.add_argument("--geo", help="Geogrid file for trimming (e.g. geo_em.d01.nc)",type=Path)
    parser.add_argument(
        "--target", help="UM file on the target grid for trimming (e.g. qrparm.mask)",type=Path
    )
    parser.add_argument(
        "--format", help="Output format", choices=["grib", "netcdf"],default="grib"
    )
    parser.add_argument(
        "--era5land",
        help="Use era5land over land",
        action=argparse.BooleanOptionalAction,
        default=True
    )
    parser.add_argument(
        "--polar",
        help="Include all longitudes",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument("--debug", help="Debug output", action="store_true")

    ns = parser.parse_args(in_args)

    handle_args(**vars(ns))
import argparse
import textwrap
import pandas
import f90nml

from pathlib import Path

from .config import conf
from .logging import log
from . import domain
from logging import DEBUG
from .main import __doc__ as maindoc

from typing import Optional, List

def handle_args(
        model: Optional[str]=None,
        file: Optional[Path]=None,
        output: Optional[Path]=None,
        namelist: Optional[Path]=None,
        geo: Optional[Path]=None,
        target: Optional[Path]=None,
        time: Optional[pandas.Timestamp]=None,
        start: Optional[pandas.Timestamp]=None,
        end: Optional[pandas.Timestamp]=None,
        format: str="grib",
        era5land: bool = True,
        polar: bool = False,
        debug: bool = False,
    ) -> None:
    
    ### Cmdline > local conf > global conf
    conf_path = Path(__file__).parent.parent / 'config'

    if model == "wrf":
        if era5land:
            conf.update(conf_path / 'wrf_era5land.yaml')
        else:
            conf.update(conf_path / 'wrf_era5.yaml')
        
        if debug:
            conf.set('log_level',DEBUG)
        log.start(conf)

        if output is None:
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
            raise KeyError("Please provide either 'start' or 'namelist' if 'wrf' is selected as a model")

        if end is None:
            end = start

        conf.set('start',start)
        conf.set('end',end)
        conf.set('polar',polar)
        conf.set('format',format)

        if geo is not None:
            conf.set('domain',domain.get_domain(Path(geo),polar))
        else:
            log.warn("Outputting the full domain, use --geo=geo_em.d01.nc to limit")
            conf.set('domain',domain.get_domain(None,polar))
    
    elif model == "um":

        if era5land:
            conf.update(conf_path / 'um_era5land.yaml')
        else:
            conf.update(conf_path / 'um_era5.yaml')

        if debug:
            conf.set('log_level',DEBUG)
        log.start(conf)
        
        time = pandas.offsets.Hour().rollback(time)
        conf.set('time',time)
        conf.set('polar',polar)
        conf.set('format',format)

        if output is None:
            output = f"um.era5.{time.strftime('%Y%m%dT%H%M')}.grib"
        
        if target is not None:
            conf.set('domain',domain.get_domain(Path(target),polar))
        else:
            log.warn("Outputting the full domain, use --target=qrparm.mask to limit")
            conf.set('domain',domain.get_domain(None,polar))

    elif model is None:
        if file is None:
            raise KeyError("If 'um' or 'wrf' is not specified, a conf file must be provided")
        conf.update(file)
        if debug:
            conf.set('log_level',DEBUG)
        log.start(conf)
        if target is not None:
            conf.set('domain',domain.get_domain(Path(target),polar))
        elif geo is not None:
            conf.set('domain',domain.get_domain(Path(geo),polar))
        else:
            log.warn("Outputting the full domain, use --target=qrparm.mask or --geo=geo_em.d01.nc to limit")
            conf.set('domain',domain.get_domain(None,polar))

    log.info(conf.get("global_config"))
    log.info(conf.get("model_config"))


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
        "--format", help="Output format", choices=["grib", "netcdf"], default="grib"
    )
    parser.add_argument(
        "--era5land",
        help="Use era5land over land",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--polar",
        help="Include all longitudes",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument("--debug", help="Debug output", action="store_true")

    ns = parser.parse_args(in_args)
    handle_args(**vars(ns))
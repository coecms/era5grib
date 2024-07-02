from pathlib import Path
import yaml
from typing import Any, Union
from .logging import log, die
import pandas
from .conftree import ConfTree

_known_configs = [ "um_era5", "um_era5land", "wrf_era5", "wrf_era5land" ]
try:
    _p = Path(__file__)
except NameError:
    die("Could not determine path to this file")

class Era5gribConfig():
    def __init__(self):
        ### First, read in global conf
        out = self.read_yaml(_p.parent / 'config' / 'defaults.yaml' )
        self.default_config = ConfTree.from_dict(out)
        self._combined_config = ConfTree.from_dict(out)

    def __contains__(self, item: Any) -> bool:
        try:
            _ = self._combined_config[item]
        except KeyError:
            return False
        return True

    def update(self,infile: Union[str,Path]):

        if infile in _known_configs:
            p = _p.parent / 'config' / f'{infile}.yaml'
        else:
            p = Path(infile)
        if not p.is_file():
            die(f"Model configuration: {infile} does not exist")
        out = self.read_yaml(p)
        ### Any includes?
        if "includes" in out:
            self.update(out["includes"])
        if hasattr(self,'model_config'):
            self.model_config.merge(out)
            self._combined_config.merge(out)
        else:
            self.model_config = ConfTree.from_dict(out)
            self._combined_config.merge(out)

    @staticmethod
    def read_yaml(fn: Path) -> dict[str,Any]:
        try:
            with open(fn, 'r') as f:
                out = yaml.safe_load(f)
        except FileNotFoundError:
            die(f"Config file {fn} not found")
        except yaml.YAMLError:
            die(f"Config file {fn} is invalid")

        return out

    def get(self,key: str,default: Any = None) -> Any:

        ### Handle some special keys:
        if key == "default_config":
            return yaml.dump(self.default_config.to_dict(),default_flow_style=False)
        elif key == "model_config":
            try:
                return yaml.dump(self.model_config.to_dict(),default_flow_style=False)
            except AttributeError as e:
                raise AttributeError("Model configuration not initialised") from e

        if not getattr(self,"model_config"):
            raise AttributeError("Model configuration not initialised") from e
        
        return self._combined_config.get(key,default)
    
    def get_time_range(self) -> pandas.DatetimeIndex:

        dr = self.get("time_range",None)
        if dr is not None:
            return dr

        log.info("Calculating time range")
        start_time = self.get("start",None)
        end_time = self.get("end",None)
        if start_time is None or end_time is None:
            die("Simulation time has not been correctly set")
        dr =  pandas.date_range(start_time,end_time,freq="h")
        self.set('time_range',dr)
        return dr

    def get_month_range(self) -> pandas.DatetimeIndex:

        mr = self.get("month_range")
        if mr is not None:
            return mr
        
        log.info("Calculating ERA5 month range")
        start_time = self.get("start",None)
        end_time = self.get("end",None)
        if start_time is None or end_time is None:
            die("Simulation time has not been correctly set")
        start_time = pandas.offsets.MonthBegin().rollback(start_time.date())
        end_time = pandas.offsets.MonthEnd().rollforward(end_time.date())
        mr = pandas.date_range(start_time,end_time,freq="ME")
        self.set("month_range",mr)
        return mr

    def set(self,key: str, val: Any) -> Any:
        
        log.debug(f"Setting model config {key}: {val}")
        self.model_config.set(key,val)
        self._combined_config.set(key,val)

    def reset(self):
        if hasattr(self,"model_config"):
            del(self.model_config)
        if hasattr(self,"_combined_config"):
            del(self._combined_config)
        ### Deep-copy original config
        self._combined_config = ConfTree.from_dict(self.default_config.to_dict())

conf = Era5gribConfig()
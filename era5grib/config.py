from pathlib import Path
import yaml
from typing import Any, Union
from .logging import log, die
import pandas

_known_configs = [ "um_era5", "um_era5land", "wrf_era5", "wrf_era5land" ]
try:
    _p = Path(__file__)
except NameError:
    die("Could not determine path to this file")
    
class Era5gribConfig():
    def __init__(self):
        ### First, read in global conf
        out = self.read_yaml(_p.parent / 'config' / 'global.yaml' )
        self.global_config = out

    def __contains__(self, item: Any) -> bool:
        if getattr(self,'model_config',None):
            return (item in self.model_config) or (item in self.global_config)
        elif getattr(self,'global_config',None):
            return item in self.global_config
        else:
            return False

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
            for k,v in out.items():
                if k in self.model_config:
                ### Recursive config gets prepended & duplicates removed if its a list
                    if isinstance(v,list):
                        self.model_config[k] = list(dict.fromkeys(v + self.model_config[k]))
                ### merged if its a dict
                    elif isinstance(v,dict):
                        self.model_config[k] = self.model_config[k] | v
                ### clobbered if its anything else (or not in the config at all)
                    else:
                        self.model_config[k] = v
                else:
                    self.model_config[k] = v
        else:
            self.model_config = out

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

        ### Recursive getter
        keys = key.split('.')

        ### key[0] gets special treatment
        k=keys[0]
        ### Handle some special keys:
        if k == "global_config":
            return yaml.dump(self.global_config,default_flow_style=False)
        elif k == "model_config":
            try:
                return yaml.dump(self.model_config,default_flow_style=False)
            except AttributeError as e:
                raise AttributeError("Model configuration not initialised") from e
        try:
            out = self.model_config.get(k,None)
        except AttributeError as e:
            raise AttributeError("Model configuration not initialised") from e
        if out is None:
            out = self.global_config.get(k,None)
        if out is None:
            out = self.global_config['defaults'].get(k,None)
        for k in keys[1:]:
            if out is not None:
                if isinstance(out,dict):
                    out = out.get(k,None)
                else:
                    out = None
        if out is not None:
            return out
        else:
            return default
    
    def get_time_range(self) -> pandas.DatetimeIndex:
        if "time_range" in self:
            return self.get("time_range")

        elif "start" in self:
            start_time = self.get("start")
            if "end" not in self:
                die("Simulation time has not been correctly set")
            end_time = self.get("end")
            dr =  pandas.date_range(start_time,end_time,freq="h")
        self.set('time_range',dr)
        return dr

    def get_month_range(self) -> pandas.DatetimeIndex:

        if "month_range" in self:
            return self.get("month_range")

        elif "start" in self:
            start_time = pandas.offsets.MonthBegin().rollback(self.get('start').date())
            if "end" not in self:
                die("Simulation time has not been correctly set")
            end_time = pandas.offsets.MonthEnd().rollforward(self.get('end').date())
            mr = pandas.date_range(start_time,end_time,freq="ME")
        self.set("month_range",mr)
        return mr

    def set(self,key: str, val: Any) -> Any:
        log.debug(f"Setting model config {key}: {val}")
        self.model_config[key]=val

conf = Era5gribConfig()
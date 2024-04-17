from pathlib import Path
import yaml
from typing import Any, Union

class Era5gribConfig():
    def __init__(self):
        
        ### First, read in global conf
        try:
            p = Path(__file__)
        except NameError:
            exit("Could not determine path to this file")
        self.read_yaml(p.parent.parent / 'config' / 'global.yaml','global_config' )
    
    def update(self,infile: Union[str,Path]):

        p = Path(infile)
        if not p.is_file():
            exit(f"Model configuration: {infile} does not exist")
        self.read_yaml(p,'model_config')
        
    def read_yaml(self,fn: Path, conf_name: str):
        try:
            with open(fn, 'r') as f:
                setattr(self,conf_name,yaml.safe_load(f))
        except FileNotFoundError:
            exit(f"Config file {fn} not found")
        except yaml.YAMLError:
            exit(f"Config file {fn} is invalid")

    def get(self,key: str) -> Any:

        ### Handle some special keys:
        if key == "global_config":
            return str(self.global_config)
        elif key == "model_config":
            try:
                return str(self.model_config)
            except AttributeError as e:
                raise AttributeError("Model configuration not initialised") from e
        try:
            out = self.model_config.get(key,None)
        except AttributeError as e:
            raise AttributeError("Model configuration not initialised") from e
        if not out:
            out = self.global_config['defaults'].get(key,None)
        if not out:
            out = self.global_config.get(key,None)
        if not out:
            raise(KeyError(f'Config has no key: {key}'))
        return out
    
    def set(self,key: str, val: Any):
        self.model_config[key]=val

conf = Era5gribConfig()
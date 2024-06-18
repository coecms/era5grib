from ..config import conf
from ..logging import log

import intake
import numpy as np

from typing import Dict,Union

class Paramdb():
    def __init__(self):

        cat_name = conf.get('metadata_catalogue','').split('.')

        for cat_path in conf.get('catalogue_paths'):
            c = intake.open_catalog(cat_path)
            for subcat in cat_name:
                if subcat in c:
                    c = c[subcat]
                else:
                    c = None
                    break

        if c is None:
            if conf.get('format') == "grib":
                log.warn("WARNING: Unable to find ECMWF metadata catalogue and GRIB format selected. GRIB field metadata will NOT correspond to input field metadata")
                self.params = None
                return

        self.params = c.read()
        self.metadata_mapping = conf.get('metadata_mapping')
    
    def __call__(self,field_name: str) -> Dict[str,Union[float,str,np.dtype[np.int32]]]:

        if self.params is None:
            return {}

        ### Expected to work on tagged or untagged types
        fn = field_name.split('_')[0]

        p = self.params[self.params.cfVarName == fn]
        if len(p) == 0:
            return {}

        p = p.iloc[0]

        out = {}
        for k,v in self.metadata_mapping.items():
            if v not in p:
                log.warn(f"WARNING: Metadata parameter {v} not found in metadata catalogue {conf.get('metadata_catalogue')}")
                continue
            if type(p[v]) == np.int64:
                out[k] = np.int32(p[v])
            else:
                out[k] = p[v]

        return out
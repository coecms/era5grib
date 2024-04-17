import logging
import sys
from . import config


class Era5GribLogger(logging.Logger):

    def __init__(self):
        super().__init__('Era5grib')
        self.started = False

    def start(self,conf: config.Era5gribConfig):

        if self.started:
            return

        try:
            ### Do we have an int logging level?
            ll = int(conf.get('log_level'))
        except ValueError:
            ### Nope
            ll = getattr(logging,conf.get('log_level').upper())
        except KeyError:
            ll = logging.WARNING
        self.addHandler(logging.StreamHandler(sys.stderr))
        self.setLevel(ll)
        self.started=True
        return
    
log = Era5GribLogger()
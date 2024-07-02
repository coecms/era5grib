import logging
import warnings
import sys
from typing import NoReturn, Union, Optional, List

_filtered_warnings=["The specified chunks separate",
                    "Sending large graph of size",
                    "Ancillary files do not define the UM version number",
                    ]
_loggers_to_override=["xarray",
                      ]

class UserWarningFilter(logging.Filter):
    def __init__(self,name=''):
        super().__init__(name)
        self.log_level = logging.root.getEffectiveLevel()
   
    def set_log_level(self,ll):
        self.log_level = ll

    def filter(self,record):
        ### Specifically reduce the level of some dask/xarray warnings coming out of 
        ### warnings.warn to INFO level
        if record.levelno == logging.WARNING and 'UserWarning' in record.getMessage():
            for f in _filtered_warnings:
                if f in record.getMessage():
                    record.levelno = logging.INFO
                    record.levelname = logging.getLevelName(logging.INFO)
        if record.levelno < self.log_level:
            return False
        return True

class Era5GribLogger(logging.Logger):

    def __init__(self):
        super().__init__('Era5grib')
        self.started = False
        self.uw_filter = UserWarningFilter()
        self.stream_handler = logging.StreamHandler(sys.stdout)
        self.stream_handler.addFilter(self.uw_filter)
        self.stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        ### Also need to add filter to logger itself - New for analysis3-24.04
        self.addFilter(self.uw_filter)

    def start(self,level: Optional[Union[str,int]]):

        if self.started:
            return

        if level:
            try:
                ### Do we have an int logging level?
                ll = int(level)
            except ValueError:
                ### Nope
                ll = level.upper()
        else:
            ll = logging.WARNING
        self.addHandler(self.stream_handler)
        try:
            self.setLevel(ll)
        except ValueError:
            self.setLevel(logging.WARNING)
            self.warning(f"Invalid logging level: {ll}")
        self.uw_filter.set_log_level(self.getEffectiveLevel())
        ### Hook up the py.warnings logger too
        ### Test if someone has already called logging.captureWarnings(True)
        self._captured_warnings = ( warnings.showwarning.__module__  == "logging" )
        ### If they have, assume they know what they're doing
        if not self._captured_warnings:
            logging.captureWarnings(True)
            warnings_logger = logging.getLogger("py.warnings")
            warnings_logger.addHandler(self.stream_handler)
        ### Other loggers we want to add our stream handler too
        for logger_name in _loggers_to_override:
                logging.getLogger(logger_name).addHandler(self.stream_handler)
                ### Also need to add filter to logger itself - New for analysis3-24.04
                logging.getLogger(logger_name).addFilter(self.uw_filter)
        self.started=True
        return
    
    def __del__(self):
        ### Clean up in case we've been called from some other __main__
        if hasattr(self,'_captured_warnings'):
            if not self._captured_warnings:
                warnings_logger = logging.getLogger("py.warnings")
                warnings_logger.removeHandler(self.stream_handler)
                logging.captureWarnings(False)
        for logger_name in _loggers_to_override:
            logging.getLogger(logger_name).removeHandler(self.stream_handler)

    def get_overridden_loggers(self) -> List[str]:
        return [ self.name, "py.warnings" ] + _loggers_to_override

log = Era5GribLogger()

def die(msg: str) -> NoReturn:
    ### Don't want the message disappearing into the void if we haven't 
    ### officially started logging yet
    if not log.started:
        log.addHandler(logging.StreamHandler(sys.stderr))
        log.setLevel(logging.WARNING)
        log._captured_warnings = True
    log.error(msg)
    sys.exit(-1)
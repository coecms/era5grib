from dask.distributed import Client
import os
import resource
import socket
from logging import ERROR

### Leave the client here in case we need it
client = None

def start_parallel() -> None:
    global client
    ### Are we in a PBS job?
    if 'PBS_ENVIRONMENT' in os.environ and not socket.gethostname().startswith('gadi-login-'):
        ### Stop trusting the environment here
        n_procs = len(os.sched_getaffinity(0))
        ### Allow each worker to use up to 90% of the total memory
        mem=resource.getrlimit(resource.RLIMIT_RSS)[0]*0.9
    else:
        ### On a login node
        n_procs=2
        mem='3GB'
    client=Client(n_workers=n_procs,threads_per_worker=1,memory_limit=mem,silence_logs=ERROR)

def stop_parallel() -> None:
    global client
    client.close()

from dask.distributed import Client
from dask.distributed.diagnostics.plugin import WorkerPlugin
import os
import resource
import socket
import logging

class CaptureWarningsPlugin(WorkerPlugin):
    def setup(self,worker):
        logging.captureWarnings(True)

    def teardown(self,worker):
        logging.captureWarnings(False)


class DaskClusterManager:

    @staticmethod
    def cpus_from_cpuset() -> int:

        ### Sensible default in case things go wrong
        default = len(os.sched_getaffinity(0))

        try:
            with open('/proc/self/cpuset','r') as f:
                cpuset_path=f.read().strip()
        except FileNotFoundError:
            return default

        try:
            with open('/sys/fs/cgroup/cpuset' + cpuset_path + '/cpuset.cpus','r') as f:
                cpu_list=f.read().strip()
        except FileNotFoundError:
            return default

        cpuset=set()
        ### Parse cpu list
        for r in cpu_list.split(','):
            if '-' in r:
                try:
                    start = int(( r_split := r.split('-') )[0])
                    end   = int(r_split[1]) + 1
                except ValueError:
                    return default
                cpuset |= set(range(start,end))
            else:
                try:
                    cpuset.add(int(r))
                except ValueError:
                    return default

        return len(cpuset)

    def __init__(self):

        self.client = None

    def __enter__(self):
        ### Are we in a PBS job?
        if 'PBS_ENVIRONMENT' in os.environ and not socket.gethostname().startswith('gadi-login-'):
            ### Stop trusting the environment here
            n_procs = self.cpus_from_cpuset()
            ### Allow each worker to use up to 90% of the total memory
            mem=resource.getrlimit(resource.RLIMIT_RSS)[0]*0.9
        else:
            ### On a login node
            n_procs=2
            mem='3GB'
        self.client=Client(n_workers=n_procs,threads_per_worker=1,memory_limit=mem)
        self.client.register_worker_plugin(CaptureWarningsPlugin())
        self.client.forward_logging()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.client.shutdown()
        del(self.client)
        self.client = None

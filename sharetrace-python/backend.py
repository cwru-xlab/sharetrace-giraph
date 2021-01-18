import contextlib
from typing import Callable

import psutil
import ray

NUM_CPUS = psutil.cpu_count(logical=False)
LOGGER: Callable = print
LOCAL_MODE = False


@contextlib.contextmanager
def ray_context(*args, **kwargs):
	try:
		yield ray.init(*args, **kwargs)
	finally:
		ray.shutdown()

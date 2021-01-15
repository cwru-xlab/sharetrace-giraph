from typing import Callable

import psutil

NUM_CPUS = psutil.cpu_count(logical=False)
LOGGER: Callable = print



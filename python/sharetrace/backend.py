import contextlib
import time
from typing import Callable

import psutil
import ray

NUM_CPUS = psutil.cpu_count(logical=False)
LOGGER: Callable = print
LOCAL_MODE = False


def set_local_mode(value: bool):
	global LOCAL_MODE
	LOCAL_MODE = value


@contextlib.contextmanager
def ray_context(*args, **kwargs):
	try:
		yield ray.init(*args, **kwargs)
	finally:
		ray.shutdown()


def get_per_task_overhead():
	@ray.remote
	def no_work(x):
		return x

	with ray_context():
		start = time.time()
		num_calls = 1000
		[ray.get(no_work.remote(x)) for x in range(num_calls)]
		overhead = (time.time() - start) * 1000 / num_calls
		print("per task overhead (ms) =", overhead)


if __name__ == '__main__':
	get_per_task_overhead()

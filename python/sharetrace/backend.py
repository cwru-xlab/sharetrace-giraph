import abc
import contextlib
import logging
import time
from typing import Callable, NoReturn

import psutil
import ray

NUM_CPUS = psutil.cpu_count(logical=False)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
STDOUT: Callable = print
STDERR: Callable = print


@contextlib.contextmanager
def ray_context(*args, **kwargs):
	try:
		yield ray.init(*args, **kwargs)
	finally:
		ray.shutdown()


def get_per_task_overhead():
	"""Prints the time overhead for a single Ray task."""

	@ray.remote
	def no_work(x):
		return x

	with ray_context():
		start = time.time()
		num_calls = 1000
		[ray.get(no_work.remote(x)) for x in range(num_calls)]
		overhead = (time.time() - start) * 1000 / num_calls
		print("per task overhead (ms) =", overhead)


def rep(cls, **attrs):
	attrs = ('{}={}'.format(k, v) for k, v in attrs.items())
	return f"{cls}({', '.join(attrs)})"


class ActorMixin(abc.ABC):
	__slots__ = []

	def __init__(self):
		super(ActorMixin, self).__init__()

	@abc.abstractmethod
	def kill(self) -> NoReturn:
		pass

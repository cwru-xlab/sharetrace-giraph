import asyncio
from typing import Any, Hashable, Iterable, Mapping, NoReturn, Optional

import ray

import backend


class Empty(Exception):
	pass


class Full(Exception):
	pass


class Queue:
	"""Extension of Queue implementation on Ray.

	Allows for a non-actor _queue using asyncio.Queue.
	"""

	__slots__ = ['local_mode', 'detached', 'max_size', '_actor']

	def __init__(
			self,
			*,
			local_mode: bool = None,
			detached: bool = True,
			max_size: int = None):
		local_mode = backend.LOCAL_MODE if local_mode is None else local_mode
		self.local_mode = bool(local_mode)
		self.detached = bool(detached)
		if max_size is None:
			self.max_size = max_size
			max_size = 0
		else:
			max_size = int(max_size)
			self.max_size = max_size
		if self.local_mode:
			self._actor = _Queue(max_size=max_size)
		else:
			self._actor = ray.remote(_Queue)
			if self.detached:
				self._actor.options(lifetime='detached')
			self._actor = self._actor.remote(max_size)

	def __len__(self):
		return self.qsize()

	def qsize(self):
		"""The size of the queue."""
		if self.local_mode:
			value = self._actor.qsize()
		else:
			value = ray.get(self._actor.qsize.remote())
		return value

	def empty(self):
		"""Whether the queue is empty."""
		if self.local_mode:
			value = self._actor.empty()
		else:
			value = ray.get(self._actor.empty.remote())
		return value

	def full(self):
		"""Whether the queue is full."""
		if self.local_mode:
			value = self._actor.full()
		else:
			value = ray.get(self._actor.full.remote())
		return value

	def put(self, item, block=True, timeout=None):
		"""Adds an item to the queue.

		There is no guarantee of order if multiple producers put to the same
		full queue.

		Raises:
			Full if the queue is full and blocking is False.
			Full if the queue is full, blocking is True, and it timed out.
			ValueError if timeout is negative.
		"""
		if not block:
			try:
				if self.local_mode:
					self._actor.put_nowait(item)
				else:
					ray.get(self._actor.put_nowait.remote(item))
			except asyncio.QueueFull:
				raise Full
		else:
			if timeout is not None and timeout < 0:
				raise ValueError("'timeout' must be a non-negative number")
			else:
				if self.local_mode:
					self._actor.put(item, timeout)
				else:
					ray.get(self._actor.put.remote(item, timeout))

	def get(self, block=True, timeout=None):
		"""Gets an item from the _queue.

		There is no guarantee of order if multiple consumers get from the
		same empty _queue.

		Returns:
			The next item in the _queue.

		Raises:
			Empty if the _queue is empty and blocking is False.
			Empty if the _queue is empty, blocking is True, and it timed out.
			ValueError if timeout is negative.
		"""
		if not block:
			try:
				if self.local_mode:
					value = self._actor.get_nowait()
				else:
					value = ray.get(self._actor.get_nowait.remote())
			except asyncio.QueueEmpty:
				raise Empty
		else:
			if timeout is not None and timeout < 0:
				raise ValueError("'timeout' must be a non-negative number")
			else:
				if self.local_mode:
					value = self._actor.get(timeout)
				else:
					value = ray.get(self._actor.get.remote(timeout))
		return value

	def put_nowait(self, item):
		"""Equivalent to put(item, block=False).

		Raises:
			Full if the _queue is full.
		"""
		return self.put(item, block=False)

	def get_nowait(self):
		"""Equivalent to get(block=False).

		Raises:
			Empty if the _queue is empty.
		"""
		return self.get(block=False)

	def kill(self):
		if not self.local_mode:
			ray.kill(self._actor)


class _Queue:
	__slots__ = ['max_size', '_queue']

	def __init__(self, max_size):
		self._queue = asyncio.Queue(max_size)

	def qsize(self):
		return self._queue.qsize()

	def empty(self):
		return self._queue.empty()

	def full(self):
		return self._queue.full()

	async def put(self, item, timeout=None):
		try:
			await asyncio.wait_for(self._queue.put(item), timeout)
		except asyncio.TimeoutError:
			raise Full

	async def get(self, timeout=None):
		try:
			return await asyncio.wait_for(self._queue.get(), timeout)
		except asyncio.TimeoutError:
			raise Empty

	def put_nowait(self, item):
		self._queue.put_nowait(item)

	def get_nowait(self):
		return self._queue.get_nowait()


class VertexStore:
	__slots__ = ['local_mode', 'detached', '_actor']

	def __init__(self, *, local_mode: bool = None, detached: bool = True):
		local_mode = backend.LOCAL_MODE if local_mode is None else local_mode
		self.local_mode = bool(local_mode)
		self.detached = bool(detached)
		if local_mode:
			self._actor = _VertexStore()
		else:
			if self.detached:
				self._actor = ray.remote(_VertexStore)
				self._actor = self._actor.options(lifetime='detached')
			else:
				self._actor = ray.remote(_VertexStore)
			self._actor = self._actor.remote()

	def get(
			self,
			key: Hashable,
			attribute: Any = None,
			as_ref: bool = False) -> Any:
		if self.local_mode:
			value = self._actor.get(key, attribute)
		else:
			value = self._actor.get.remote(key, attribute)
		return value if as_ref or self.local_mode else ray.get(value)

	def put(
			self,
			keys: Iterable[Hashable],
			attributes: Mapping[Hashable, Any] = None,
			as_ref: bool = False) -> Optional[ray.ObjectRef]:
		if self.local_mode:
			self._actor.put(keys, attributes)
			value = None
		else:
			value = self._actor.put.remote(keys, attributes)
		return value if as_ref or self.local_mode else ray.get(value)

	def kill(self) -> NoReturn:
		if not self.local_mode:
			ray.kill(self._actor)


class _VertexStore:
	__slots__ = ['_store']

	def __init__(self):
		self._store = {}

	def get(self, key: Hashable, attribute: Any = None) -> Any:
		if attribute is None:
			value = self._store[key]
			value = key if value is None else value
		else:
			value = self._store[key][attribute]
		return value

	def put(
			self,
			keys: Iterable[Hashable],
			attributes: Optional[Any] = None) -> NoReturn:
		if attributes is None:
			self._store.update(dict.fromkeys(keys, None))
		elif isinstance(attributes, Mapping):
			for k in keys:
				if k in self._store:
					self._store[k].update(attributes[k])
				else:
					self._store[k] = attributes[k]
		else:
			self._store.update(attributes)

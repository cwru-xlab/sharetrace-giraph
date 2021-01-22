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

	Allows for a non-actor queue using asyncio.Queue.
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
		qsize = self._actor.qsize
		return qsize() if self.local_mode else ray.get(qsize.remote())

	def empty(self):
		"""Whether the queue is empty."""
		empty = self._actor.empty
		return empty() if self.local_mode else ray.get(empty.remote())

	def full(self):
		"""Whether the queue is full."""
		full = self._actor.full
		return full() if self.local_mode else ray.get(full.remote())

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
				put = self._actor.put_nowait
				put(item) if self.local_mode else put.remote(item)
			except asyncio.QueueFull:
				raise Full
		else:
			if timeout is not None and timeout < 0:
				raise ValueError("'timeout' must be a non-negative number")
			else:
				put = self._actor.put
				if self.local_mode:
					put(item, timeout)
				else:
					put.remote(item, timeout)

	def get(self, block=True, timeout=None):
		"""Gets an item from the queue.

		There is no guarantee of order if multiple consumers get from the
		same empty queue.

		Returns:
			The next item in the queue.

		Raises:
			Empty if the queue is empty and blocking is False.
			Empty if the queue is empty, blocking is True, and it timed out.
			ValueError if timeout is negative.
		"""
		if block:
			if timeout is not None and timeout < 0:
				raise ValueError("'timeout' must be a non-negative number")
			else:
				get = self._actor.get
				if self.local_mode:
					value = get(timeout)
				else:
					value = ray.get(get.remote(timeout))
		else:
			try:
				get = self._actor.get_nowait
				value = get() if self.local_mode else ray.get(get.remote())
			except asyncio.QueueEmpty:
				raise Empty

		return value

	def put_nowait(self, item):
		"""Equivalent to put(item, block=False).

		Raises:
			Full if the queue is full.
		"""
		return self.put(item, block=False)

	def get_nowait(self):
		"""Equivalent to get(block=False).

		Raises:
			Empty if the queue is empty.
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
		if self.local_mode:
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
			*,
			key: Hashable,
			attribute: Any = None,
			as_ref: bool = False) -> Any:
		get = self._actor.get
		if self.local_mode:
			value = get(key, attribute)
		else:
			value = get.remote(key, attribute)
		return value if as_ref or self.local_mode else ray.get(value)

	def put(
			self,
			*,
			keys: Iterable[Hashable],
			attributes: Mapping[Hashable, Any] = None,
			merge: bool = False) -> NoReturn:
		put = self._actor.put
		if self.local_mode:
			put(keys, attributes, merge)
		else:
			put.remote(keys, attributes, merge)

	def kill(self) -> NoReturn:
		if not self.local_mode:
			ray.kill(self._actor)

	def __copy__(self):
		store = VertexStore(local_mode=True)
		if self.local_mode:
			store._actor = self._actor
		else:
			store._actor = ray.get(self._actor.copy.remote())
		return store


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

	# TODO This is convoluted
	def put(
			self,
			keys: Iterable[Hashable],
			attributes: Optional[Any] = None,
			merge: bool = False) -> NoReturn:
		if attributes is None:
			self._store.update(dict.fromkeys(keys, None))
		elif isinstance(attributes, Mapping):
			for k in keys:
				if k in self._store:
					if isinstance(attributes[k], Mapping):
						if merge:
							for a in attributes[k]:
								previous = self._store[k][a]
								current = {**previous, **attributes[k][a]}
								self._store[k][a] = current
						else:
							for a in attributes[k]:
								self._store[k][a] = attributes[k][a]
					else:
						self._store[k].update(attributes[k])
				else:
					self._store[k] = attributes[k]
		else:
			self._store.update(attributes)

	def copy(self):
		vertex_store = _VertexStore()
		vertex_store._store = self._store.copy()
		return vertex_store

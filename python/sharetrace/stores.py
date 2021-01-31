import abc
import asyncio
import collections
from typing import (
	Any, Hashable, Iterable, Iterator, Mapping, NoReturn, Optional)

import ray
from ray.util import queue

import backend


class Queue(abc.ABC, collections.Sized):
	__slots__ = []

	def __init__(self):
		super(Queue, self).__init__()

	def __repr__(self):
		return backend.rep(self.__class__.__name__)

	@property
	@abc.abstractmethod
	def max_size(self) -> int:
		pass

	@abc.abstractmethod
	def empty(self) -> bool:
		pass

	@abc.abstractmethod
	def full(self) -> bool:
		pass

	@abc.abstractmethod
	def put(
			self,
			*items: Any,
			block: bool = True,
			timeout: Optional[float] = None) -> bool:
		pass

	@abc.abstractmethod
	def get(
			self,
			*,
			block: bool = True,
			timeout: Optional[float] = None) -> Any:
		pass


class LocalQueue(Queue):
	"""Simple FIFO queue."""
	__slots__ = ['_max_size', '_queue']

	def __init__(self, max_size: int = 0):
		super(LocalQueue, self).__init__()
		self._max_size = None if max_size in {None, 0} else int(max_size)
		self._queue = collections.deque(maxlen=self.max_size)

	def __repr__(self):
		return backend.rep(self.__class__.__name__, max_size=self.max_size)

	def __len__(self) -> int:
		return len(self._queue)

	@property
	def max_size(self) -> int:
		return self._max_size

	def empty(self) -> bool:
		return len(self._queue) == 0

	def full(self) -> bool:
		return len(self._queue) == self._queue.maxlen

	def put(
			self,
			*items: Any,
			block: bool = True,
			timeout: Optional[float] = None) -> bool:
		self._queue.extend(items)
		return True

	def get(
			self,
			*,
			block: bool = True,
			timeout: Optional[float] = None) -> Any:
		return self._queue.popleft()


class AsyncQueue(Queue):
	"""Asynchronous queue."""
	__slots__ = ['_max_size', '_queue']

	def __init__(self, max_size: int = 0):
		super(AsyncQueue, self).__init__()
		self._max_size = int(max_size)
		self._queue = asyncio.Queue(maxsize=max_size)

	def __repr__(self):
		return backend.rep(self.__class__.__name__, max_size=self.max_size)

	def __len__(self):
		return self._queue.qsize()

	@property
	def max_size(self) -> int:
		return self._max_size

	def empty(self) -> bool:
		return self._queue.empty()

	def full(self) -> bool:
		return self._queue.full()

	async def put(
			self,
			*items: Any,
			block: bool = True,
			timeout: Optional[float] = None) -> bool:
		if block:
			for item in items:
				future = self._queue.put(item)
				await asyncio.wait_for(future, timeout=timeout)
		else:
			for item in items:
				self._queue.put_nowait(item)
		return True

	async def get(
			self,
			*,
			block: bool = True,
			timeout: Optional[float] = None) -> Any:
		if block:
			future = self._queue.get()
			item = await asyncio.wait_for(future, timeout=timeout)
		else:
			item = self._queue.get_nowait()
		return item


class RemoteQueue(Queue, backend.ActorMixin):
	"""FIFO Ray actor queue."""
	__slots__ = ['_max_size', '_actor']

	def __init__(self, max_size: int = 0):
		super(RemoteQueue, self).__init__()
		self._max_size = int(max_size)
		self._actor = queue.Queue(maxsize=max_size)

	def __repr__(self):
		return backend.rep(self.__class__.__name__, max_size=self.max_size)

	def __len__(self) -> int:
		return self._actor.qsize()

	@property
	def max_size(self) -> int:
		return self._max_size

	def empty(self) -> bool:
		return self._actor.empty()

	def full(self) -> bool:
		return self._actor.full()

	def put(
			self,
			*items: Any,
			block: bool = True,
			timeout: Optional[float] = None) -> bool:
		for item in items:
			self._actor.put(item, block=block, timeout=timeout)
		return True

	def get(
			self,
			*,
			block: bool = True,
			timeout: Optional[float] = None) -> Any:
		return self._actor.get(block=block, timeout=timeout)

	def kill(self) -> NoReturn:
		ray.kill(self._actor.actor)


class VertexStore(collections.Collection, backend.ActorMixin):
	"""Data structure for storing vertex data

	Attributes:
		local_mode: True uses an in-memory, single-process implementation.
			False makes the VertexStore a Ray actor on a separate process.
		detached: True will prevent the Ray actor VertexStore from being
			terminated. Only active if local_mode is False.

	Examples:
		A vertex store can be used for the following four scenarios:

		1. Vertices with no attributes:
			{<id>: None...}

		2. Vertices with a scalar attribute:
			{<id>: <attr> ...}

		3. Vertices with multiple scalar attributes:
			{<id>: {<attr_key>: <attr_value>...}...}

		4. Vertices with mapping attributes:
			{<id>: {<attr_key>: {<key>: <value>...}...}...}
	"""

	__slots__ = ['local_mode', 'detached', '_actor']

	def __init__(self, *, local_mode: bool = True, detached: bool = True):
		super(VertexStore, self).__init__()
		self.local_mode = bool(local_mode)
		self.detached = bool(detached)
		if self.local_mode:
			self._actor = _VertexStore()
		else:
			self._actor = ray.remote(_VertexStore)
			if self.detached:
				self._actor = self._actor.options(lifetime='detached')
			self._actor = self._actor.remote()

	def __repr__(self):
		return backend.rep(
			self.__class__.__name__,
			local_mode=self.local_mode,
			detached=self.detached)

	def __iter__(self) -> Iterator:
		return iter(self._actor)

	def __next__(self) -> Any:
		iterable = iter(self._actor)
		yield next(iterable)

	def __len__(self) -> int:
		return len(self._actor)

	def __contains__(self, __x: object) -> bool:
		return __x in self._actor

	def get(
			self,
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


class _VertexStore(collections.Collection):
	__slots__ = ['_store']

	def __init__(self):
		self._store = {}

	def __iter__(self) -> Iterator:
		return iter(self._store)

	def __next__(self):
		iterable = iter(self)
		yield next(iterable)

	def __len__(self) -> int:
		return len(self._store)

	def __contains__(self, __x: object) -> bool:
		return __x in self._store

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
			attributes: Mapping[Hashable, Any] = None,
			merge: bool = False) -> NoReturn:
		def combine(key):
			for a in attributes[key]:
				updated = {**self._store[key][a], **attributes[key][a]}
				self._store[k][a] = updated

		def replace(key):
			for a in attributes[key]:
				self._store[key][a] = attributes[key][a]

		if attributes is None:
			self._store.update(dict.fromkeys(keys, None))
		else:
			for k in keys:
				if k in self._store and isinstance(attributes[k], Mapping):
					combine(k) if merge else replace(k)
				else:
					self._store[k] = attributes[k]


def queue_factory(
		max_size: int = 0, *,
		asynchronous: bool = False,
		local_mode: bool = True) -> Queue:
	if local_mode:
		if asynchronous:
			queue_obj = AsyncQueue(max_size)
		else:
			queue_obj = LocalQueue(max_size)
	else:
		queue_obj = RemoteQueue(max_size)
	return queue_obj

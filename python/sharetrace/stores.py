import abc
import collections
from typing import Any, Hashable, Iterable, Mapping, NoReturn, Optional

import ray
from ray.util import queue

import backend

_KILL_EXCEPTION = '{} does not support kill(); use {} instead.'


class Empty(Exception):
	pass


class Full(Exception):
	pass


class Queue(abc.ABC):
	__slots__ = []

	def __init__(self):
		super(Queue, self).__init__()

	@abc.abstractmethod
	def __len__(self) -> int:
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
			item: Any,
			*,
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

	@abc.abstractmethod
	def kill(self) -> NoReturn:
		pass


class LocalQueue(Queue):
	"""FIFO queue implementation using collections.deque."""
	__slots__ = ['max_size', '_queue']

	def __init__(self, max_size: int = 0):
		super(LocalQueue, self).__init__()
		self.max_size = None if max_size in {None, 0} else int(max_size)
		self._queue = collections.deque(maxlen=self.max_size)

	def __len__(self) -> int:
		return len(self._queue)

	def empty(self) -> bool:
		return len(self._queue) == 0

	def full(self) -> bool:
		return len(self._queue) == self._queue.maxlen

	def put(
			self,
			item: Any,
			*,
			block: bool = True,
			timeout: Optional[float] = None) -> bool:
		self._queue.append(item)
		return True

	def get(
			self,
			*,
			block: bool = True,
			timeout: Optional[float] = None) -> Any:
		return self._queue.popleft()

	def kill(self) -> NoReturn:
		msg = _KILL_EXCEPTION.format(self.__class__.__name__, 'RemoteQueue')
		raise NotImplementedError(msg)


class RemoteQueue(Queue):
	__slots__ = ['max_size', '_actor']

	def __init__(self, max_size: int = 0):
		super(RemoteQueue, self).__init__()
		self.max_size = int(max_size)
		self._actor = queue.Queue(maxsize=max_size)

	def __len__(self) -> int:
		return self._actor.qsize()

	def empty(self) -> bool:
		return self._actor.empty()

	def full(self) -> bool:
		return self._actor.full()

	def put(
			self,
			item: Any,
			*,
			block: bool = True,
			timeout: Optional[float] = None) -> bool:
		return self._actor.put(item, block=block, timeout=timeout)

	def get(
			self,
			*,
			block: bool = True,
			timeout: Optional[float] = None) -> Any:
		return self._actor.get(block=block, timeout=timeout)

	def kill(self) -> NoReturn:
		ray.kill(self._actor.actor)


class VertexStore:
	"""Data structure for storing vertex data

	Examples:
		A vertex store can be used for the following four scenarios:

		(1) Vertices with no attributes:
			{<id>: None...}

		(2) Vertices with a scalar attribute:
			{<id>: <attr> ...}

		(2) Vertices with multiple scalar attributes:
			{<id>: {<attr_key>: <attr_value>...}...}

		(4) Vertices with mapping attributes:
			{<id>: {<attr_key>: {<key>: <value>...}...}...}
	"""
	__slots__ = ['local_mode', 'detached', '_actor']

	def __init__(self, *, local_mode: bool = None, detached: bool = True):
		local_mode = backend.LOCAL_MODE if local_mode is None else local_mode
		self.local_mode = bool(local_mode)
		self.detached = bool(detached)
		if self.local_mode:
			self._actor = _VertexStore()
		else:
			self._actor = ray.remote(_VertexStore)
			if self.detached:
				self._actor = self._actor.options(lifetime='detached')
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


def queue_factory(max_size: int = 0, *, local_mode: bool = None) -> Queue:
	local_mode = backend.LOCAL_MODE if local_mode is None else local_mode
	if local_mode:
		queue_obj = LocalQueue(max_size=max_size)
	else:
		queue_obj = RemoteQueue(max_size=max_size)
	return queue_obj

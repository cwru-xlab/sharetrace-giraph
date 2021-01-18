import datetime
from typing import Hashable, Iterable

import attr
import numpy as np

"""IMPORTANT: Order of attributes affects attr 'order' attribute"""


@attr.s(slots=True, frozen=True, order=True)
class RiskScore:
	value = attr.ib(type=float, converter=float)
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=attr.validators.instance_of(datetime.datetime))
	name = attr.ib(type=Hashable, default='')

	def as_array(self):
		dt = np.dtype([
			('name', 'U128'),
			('timestamp', 'datetime64[s]'),
			('value', 'float64')])
		return np.array([(self.name, self.timestamp, self.value)], dtype=dt)

	@classmethod
	def from_array(cls, a: np.ndarray) -> 'RiskScore':
		return RiskScore(
			name=a['name'],
			timestamp=_from_datetime64(a['timestamp']),
			value=a['value'])


@attr.s(slots=True, frozen=True, order=True)
class TemporalLocation:
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=attr.validators.instance_of(datetime.datetime))
	location = attr.ib(type=Hashable)


@attr.s(slots=True, frozen=True)
class LocationHistory:
	name = attr.ib(type=Hashable)
	history = attr.ib(type=Iterable[TemporalLocation], converter=frozenset)


@attr.s(slots=True, frozen=True, order=True)
class Occurrence:
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=attr.validators.instance_of(datetime.datetime))
	duration = attr.ib(
		type=datetime.timedelta,
		validator=attr.validators.instance_of(datetime.timedelta))

	def as_array(self) -> np.ndarray:
		dt = np.dtype([
			('timestamp', 'datetime64[s]'), ('duration', 'timedelta64[s]')])
		return np.array([(self.timestamp, self.duration)], dtype=dt)

	@classmethod
	def from_array(cls, a: np.ndarray) -> 'Occurrence':
		return Occurrence(
			timestamp=_from_datetime64(a['timestamp']),
			duration=_from_timedelta64(a['duration']))


@attr.s(slots=True, frozen=True)
class Contact:
	users = attr.ib(type=Iterable[Hashable], converter=frozenset)
	occurrences = attr.ib(type=Iterable[Occurrence], converter=frozenset)

	def __attrs_post_init__(self):
		if len(self.users) != 2:
			raise AttributeError('Contact must have 2 distinct users')

	@classmethod
	def from_array(cls, users: Iterable[Hashable], a: np.ndarray) -> 'Contact':
		occurrences = (Occurrence.from_array(o) for o in np.nditer(a))
		return Contact(users=users, occurrences=occurrences)

	def as_array(self) -> np.ndarray:
		array = np.array([o.as_array() for o in self.occurrences]).flatten()
		array.sort(order=['timestamp', 'duration'])
		return array


def _from_datetime64(timestamp: np.datetime64) -> datetime.datetime:
	return datetime.datetime.utcfromtimestamp(np.float64(timestamp))


def _from_timedelta64(duration: np.timedelta64) -> datetime.timedelta:
	return datetime.timedelta(seconds=np.float64(duration))

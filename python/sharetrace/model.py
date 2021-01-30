import datetime
from typing import Any, Hashable, Iterable

import attr
import numpy as np


# Order of attributes affects attr 'order' attribute


@attr.s(slots=True, frozen=True, order=True)
class RiskScore:
	"""An identifiable temporal score of risk.

	Notes:
		Order is determined in the following order:
			(1) value
			(2) timestamp
			(3) name
	"""
	value = attr.ib(type=float, converter=float, kw_only=True)
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=attr.validators.instance_of(datetime.datetime),
		kw_only=True)
	name = attr.ib(type=Hashable, default='', kw_only=True)

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
	"""A time-location pair.

	Notes:
		Order is determined in the following order:
		 (1) timestamp
		 (2) location
	"""
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=attr.validators.instance_of(datetime.datetime),
		kw_only=True)
	location = attr.ib(type=Hashable, kw_only=True)


@attr.s(slots=True, frozen=True)
class LocationHistory:
	"""An identifiable set of time-location pairs."""
	name = attr.ib(type=Hashable, kw_only=True)
	history = attr.ib(
		type=Iterable[TemporalLocation], converter=frozenset, kw_only=True)


@attr.s(slots=True, frozen=True, order=True)
class Occurrence:
	"""A time-duration pair.

	Notes:
		Order is first determined in the following order:
			(1) timestamp
			(2) duration
	"""
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=attr.validators.instance_of(datetime.datetime),
		kw_only=True)
	duration = attr.ib(
		type=datetime.timedelta,
		validator=attr.validators.instance_of(datetime.timedelta),
		kw_only=True)

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
	"""A set of zero or more occurrences between two users.

	Attributes:
		users: The names of the two individuals. To allow the users to be
			used in a Mapping, the user type must be hashable.
		occurrences: A set of zero or more time-duration pairs that define
			the details of the contact.
	"""
	users = attr.ib(
		type=Iterable[Hashable], converter=frozenset, kw_only=True)
	occurrences = attr.ib(
		type=Iterable[Occurrence], converter=frozenset, kw_only=True)

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


@attr.s(slots=True, frozen=True)
class Message:
	"""A message with knowledge of the sender and receiver.

	Attributes:
		sender: The one sending the message.
		receiver: The one receiving the message.
		content: The information being sent.
	"""
	sender = attr.ib(type=Hashable, kw_only=True)
	receiver = attr.ib(type=Hashable, kw_only=True)
	content = attr.ib(type=Any, kw_only=True)


def _from_datetime64(timestamp: np.datetime64) -> datetime.datetime:
	timestamp = np.float64(np.datetime64(timestamp, 's'))
	return datetime.datetime.utcfromtimestamp(timestamp)


def _from_timedelta64(duration: np.timedelta64) -> datetime.timedelta:
	return datetime.timedelta(seconds=np.float64(duration))

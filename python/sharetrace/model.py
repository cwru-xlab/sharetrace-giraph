import datetime
import numbers
from typing import Any, Hashable, Iterable

import attr
import numpy as np
from attr import validators


# Order of attributes affects attr 'order' attribute


# noinspection PyUnresolvedReferences
@attr.s(slots=True, frozen=True, order=True)
class RiskScore:
	"""An identifiable temporal score of risk.

	Notes:
		Order is determined in the following order:
			1. value
			2. timestamp
			3. name

	Attributes:
		value: Numeric magnitude of the risk score.
		timestamp: Time associated with the risk score.
		name: Identity of the risk score.
	"""
	value = attr.ib(
		type=float,
		validator=validators.instance_of(numbers.Real),
		converter=float,
		kw_only=True)
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=validators.instance_of(datetime.datetime),
		kw_only=True)
	name = attr.ib(
		type=Hashable,
		validator=validators.instance_of(Hashable),
		default='',
		kw_only=True)

	def as_array(self):
		"""Returns the risk score as a numpy structured array.

		Note that the timestamp is at the second resolution.
		"""
		dt = np.dtype([
			('name', 'U128'),
			('timestamp', 'datetime64[s]'),
			('value', 'float64')])
		return np.array([(self.name, self.timestamp, self.value)], dtype=dt)

	@classmethod
	def from_array(cls, a: np.ndarray) -> 'RiskScore':
		return RiskScore(
			name=_unwrap_value(a['name']),
			timestamp=_from_datetime64(_unwrap_value(a['timestamp'])),
			value=_unwrap_value(a['value']))


# noinspection PyUnresolvedReferences
@attr.s(slots=True, frozen=True, order=True)
class TemporalLocation:
	"""A time-location pair.

	Order is determined in the following order:
		1. timestamp
		2. location

	Attributes:
		timestamp: The temporal aspect of the location.
		location: The spatial aspect of the timestamp.
	"""
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=validators.instance_of(datetime.datetime),
		kw_only=True)
	location = attr.ib(
		type=Hashable,
		validator=validators.instance_of(Hashable),
		kw_only=True)


@attr.s(slots=True, frozen=True)
class LocationHistory:
	"""An identifiable set of time-location pairs."""
	name = attr.ib(
		type=Hashable,
		validator=validators.instance_of(Hashable),
		kw_only=True)
	history = attr.ib(
		type=Iterable[TemporalLocation],
		validator=validators.deep_iterable(
			validators.instance_of(TemporalLocation),
			validators.instance_of(Iterable)),
		converter=frozenset,
		kw_only=True)


# noinspection PyUnresolvedReferences
@attr.s(slots=True, frozen=True, order=True)
class Occurrence:
	"""A time-duration pair.

	Notes:
		Order is first determined in the following order:
			1. timestamp
			2. duration

	Attributes:
		timestamp: Time of the occurrence.
		duration: The time period that the occurrence lasts.
	"""
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=validators.instance_of(datetime.datetime),
		kw_only=True)
	duration = attr.ib(
		type=datetime.timedelta,
		validator=validators.instance_of(datetime.timedelta),
		kw_only=True)

	def as_array(self) -> np.ndarray:
		"""Represents the occurrence as a structured numpy array.

		Note that both timestamp and duration are at the second resolution.
		"""
		dt = np.dtype([
			('timestamp', 'datetime64[s]'), ('duration', 'timedelta64[s]')])
		return np.array([(self.timestamp, self.duration)], dtype=dt)

	@classmethod
	def from_array(cls, a: np.ndarray) -> 'Occurrence':
		return Occurrence(
			timestamp=_from_datetime64(_unwrap_value(a['timestamp'])),
			duration=_from_timedelta64(_unwrap_value(a['duration'])))


# noinspection PyUnresolvedReferences
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
		type=Iterable[Hashable],
		validator=validators.deep_iterable(
			validators.instance_of(Hashable),
			validators.instance_of(Iterable)),
		converter=frozenset,
		kw_only=True)
	occurrences = attr.ib(
		type=Iterable[Occurrence],
		validator=validators.deep_iterable(
			validators.instance_of(Occurrence),
			validators.instance_of(Iterable)),
		converter=frozenset,
		kw_only=True)

	def __attrs_post_init__(self):
		if len(self.users) != 2:
			raise AttributeError('Contact must have 2 distinct users')

	@classmethod
	def from_array(cls, users: Iterable[Hashable], a: np.ndarray) -> 'Contact':
		occurrences = (Occurrence.from_array(o) for o in np.nditer(a))
		return Contact(users=users, occurrences=occurrences)

	def as_array(self) -> np.ndarray:
		"""Represents the Contact as a structured numpy array.

		Note that only the occurrences of the contact are returned and sorted
		first based on timestamp and then by duration, in ascending order.
		"""
		array = np.array([o.as_array() for o in self.occurrences]).flatten()
		array.sort(order=['timestamp', 'duration'])
		return array


# noinspection PyUnresolvedReferences
@attr.s(slots=True, frozen=True)
class Message:
	"""A message with knowledge of the sender and receiver.

	Attributes:
		sender: The one sending the message.
		receiver: The one receiving the message.
		content: The information being sent.
	"""
	sender = attr.ib(
		type=Hashable,
		validator=validators.instance_of(Hashable),
		kw_only=True)
	receiver = attr.ib(
		type=Hashable,
		validator=validators.instance_of(Hashable),
		kw_only=True)
	content = attr.ib(type=Any, kw_only=True)


def _unwrap_value(value: Any):
	"""Returns value of a singleton numpy array."""
	if isinstance(value, np.ndarray):
		value = value[0]
	return value


def _from_datetime64(timestamp: np.datetime64) -> datetime.datetime:
	timestamp = np.float64(np.datetime64(timestamp, 's'))
	return datetime.datetime.utcfromtimestamp(timestamp)


def _from_timedelta64(duration: np.timedelta64) -> datetime.timedelta:
	duration = np.float64(np.timedelta64(duration, 's'))
	return datetime.timedelta(seconds=duration)

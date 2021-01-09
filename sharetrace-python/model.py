import datetime
from typing import Hashable, Set

import attr


@attr.s(slots=True, frozen=True, order=True)
class RiskScore:
	value = attr.ib(type=float)
	timestamp = attr.ib(type=datetime.time)
	id = attr.ib(type=Hashable)

	@value.validator
	def check_value(self, attribute, value):
		if value < 0 or value > 1:
			raise ValueError('Value must be between 0 and 1, inclusive')


@attr.s(slots=True, frozen=True, order=True)
class TemporalLocation:
	timestamp = attr.ib(type=datetime.datetime)
	location = attr.ib(type=Hashable)


@attr.s(slots=True, frozen=True)
class LocationHistory:
	id = attr.ib(type=Hashable)
	history = attr.ib(type=Set[TemporalLocation])


@attr.s(slots=True, frozen=True, order=True)
class Occurrence:
	timestamp = attr.ib(type=datetime.datetime)
	duration = attr.ib(type=datetime.timedelta)


@attr.s(slots=True, frozen=True)
class Contact:
	users = attr.ib(type=Set[Hashable])
	occurrences = attr.ib(type=Set[Occurrence])

	def __attrs_post_init__(self):
		if len(self.users) != 2:
			raise AttributeError('Contact must have 2 distinct users')

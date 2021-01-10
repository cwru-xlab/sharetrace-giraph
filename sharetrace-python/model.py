import datetime
from typing import Hashable, Iterable

import attr

"""IMPORTANT: Order of attributes affects attr order attribute"""


@attr.s(slots=True, frozen=True, order=True)
class RiskScore:
	value = attr.ib(type=float, converter=float)
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=attr.validators.instance_of(datetime.datetime))
	id = attr.ib(type=Hashable)

	@value.validator
	def check_value(self, attribute, value):
		if value < 0 or value > 1:
			raise ValueError('Value must be between 0 and 1, inclusive')


@attr.s(slots=True, frozen=True, order=True)
class TemporalLocation:
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=attr.validators.instance_of(datetime.datetime))
	location = attr.ib(type=Hashable)


@attr.s(slots=True, frozen=True)
class LocationHistory:
	id = attr.ib(type=Hashable)
	history = attr.ib(type=Iterable[TemporalLocation], converter=frozenset)


@attr.s(slots=True, frozen=True, order=True)
class Occurrence:
	timestamp = attr.ib(
		type=datetime.datetime,
		validator=attr.validators.instance_of(datetime.datetime))
	duration = attr.ib(
		type=datetime.timedelta,
		validator=attr.validators.instance_of(datetime.timedelta))


@attr.s(slots=True, frozen=True)
class Contact:
	users = attr.ib(type=Iterable[Hashable], converter=frozenset)
	occurrences = attr.ib(type=Iterable[Occurrence], converter=frozenset)

	def __attrs_post_init__(self):
		if len(self.users) != 2:
			raise AttributeError('Contact must have 2 distinct users')

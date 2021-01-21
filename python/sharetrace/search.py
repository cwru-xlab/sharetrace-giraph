import datetime
import itertools
import random
from typing import Iterable, Optional

import attr
import codetiming
import numpy as np

import backend
import model

stdout = backend.STDOUT
stderr = backend.STDERR
_MIN_DURATION = datetime.timedelta(minutes=15)
Histories = Iterable[model.LocationHistory]
Contacts = Iterable[model.Contact]
Occurrences = Iterable[model.Occurrence]


@attr.s(slots=True, frozen=True)
class ContactSearch:
	"""
	Given two LocationHistory instances, this algorithm finds a contact if one
	exists. Two users must be in the same location for a sufficiently long
	period of time for them to be considered in contact.

	The algorithm takes as input an iterable of LocationHistory instances,
	each belonging to a distinct user. Since a Contact is symmetric, only the
	unique pairs are considered to prevent redundant computation.

	This algorithm iterates through both histories to find all occurrences
	For any given entry, if both LocationHistory instances have the same
	location and the occurrence has not begun, the start of the occurrence is
	the later of the two instances. It is assumed that the user is in the same
	location until the next location is recorded. Once the occurrence has
	begun, both LocationHistory instances are iterated until the histories
	differ; this marks the end of the occurrence. The earlier of the two
	LocationHistory instances is used to indicate the end of the occurrence.
	If this interval is long enough, the interval is recorded as an official
	occurrence. This is repeated for all uniques pairs supplied as input to
	the algorithm. In the case that the end of either LocationHistory
	is reached and the occurrence has begun, the same check is made to verify
	if the occurrence is long enough.

	Given a collection of LocationHistory instances of size N, the iteration
	over all unique pairs takes O(N(N - 1) / 2) = O(N^2). Given two
	LocationHistory instances of size H1 and H2, the time to find all
	occurrences is O(max(H1, H2)). The overall running time is
	O(max(H1, H2) + N^2). Given that the iterable of LocationHistory
	instances can be partitioned, the map-reduce paradigm can be applied to
	apply the algorithm to each partition and collect all of the resulting
	Contact instances.
	"""
	min_duration = attr.ib(
		type=datetime.timedelta,
		validator=attr.validators.instance_of(datetime.timedelta),
		default=_MIN_DURATION,
		kw_only=True)

	def __call__(
			self,
			histories: Histories,
			*,
			as_iterator: bool = True) -> Contacts:
		stdout('------------START CONTACT MATCHING------------')
		result = self._call(histories, as_iterator)
		stdout('-------------END CONTACT MATCHING-------------')
		return result

	@codetiming.Timer('Total duration: {:0.6f} s', logger=stdout)
	def _call(self, histories: Histories, as_iterator: bool) -> Contacts:
		pairs = itertools.combinations(histories, 2)
		contacts = (self._find_contact(*p) for p in pairs)
		contacts = (c for c in contacts if len(c.occurrences) > 0)
		if not as_iterator:
			contacts = np.array(list(contacts))
		return contacts

	def _find_contact(
			self,
			h1: model.LocationHistory,
			h2: model.LocationHistory) -> model.Contact:
		users = {h1.name, h2.name}
		occurrences = self._find_occurrences(h1, h2)
		return model.Contact(users=users, occurrences=occurrences)

	def _find_occurrences(
			self,
			h1: model.LocationHistory,
			h2: model.LocationHistory) -> Occurrences:
		occurrences = set()
		if len(h1.history) == 0 and len(h2.history) == 0 or h1.name == h2.name:
			return occurrences
		iter1 = iter(sorted(h1.history))
		iter2 = iter(sorted(h2.history))
		loc1 = next(iter1)
		loc2 = next(iter2)
		next1 = next(iter1, None)
		next2 = next(iter2, None)
		started = False
		start = self._get_later(loc1, loc2)
		while next1 is not None and next2 is not None:
			if loc1.location == loc2.location:
				if started:
					loc1 = next1
					next1 = next(iter1, None)
					loc2 = next2
					next2 = next(iter2, None)
				else:
					started = True
					start = self._get_later(loc1, loc2)
			else:
				if started:
					started = False
					occur = self._create_occurrence(start, loc1, loc2)
					if occur is not None:
						occurrences.add(occur)
				else:
					if loc1.timestamp < loc2.timestamp:
						loc1 = next1
						next1 = next(iter1, None)
					elif loc2.timestamp < loc1.timestamp:
						loc2 = next2
						next2 = next(iter2, None)
					else:
						if random.randint(1, 2) == 1:
							loc1 = next1
							next1 = next(iter1, None)
						else:
							loc2 = next2
							next2 = next(iter2, None)
		if started:
			occur = self._create_occurrence(start, loc1, loc2)
			if occur is not None:
				occurrences.add(occur)
		return occurrences

	def _create_occurrence(
			self,
			start: model.TemporalLocation,
			loc1: model.TemporalLocation,
			loc2: model.TemporalLocation) -> Optional[model.Occurrence]:
		end = ContactSearch._get_earlier(loc1, loc2)
		duration = end.timestamp - start.timestamp
		if duration >= self.min_duration:
			occurrence = model.Occurrence(
				timestamp=start.timestamp, duration=duration)
		else:
			occurrence = None
		return occurrence

	@staticmethod
	def _get_later(loc1: model.TemporalLocation, loc2: model.TemporalLocation):
		return loc1 if loc1.timestamp > loc2.timestamp else loc2

	@staticmethod
	def _get_earlier(
			loc1: model.TemporalLocation, loc2: model.TemporalLocation):
		return loc1 if loc1.timestamp < loc2.timestamp else loc2

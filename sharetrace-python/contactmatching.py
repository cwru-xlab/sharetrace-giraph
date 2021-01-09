import datetime
import itertools
import random
from typing import Sequence, Set

import model

MIN_DURATION = datetime.timedelta(minutes=15)


def compute(locations: Sequence[model.LocationHistory]):
	pairs = itertools.combinations(locations, 2)
	contacts = (_find_contact(*pair) for pair in pairs)
	return set(c for c in contacts if len(c.occurrences) > 0)


def _find_contact(
		h1: model.LocationHistory, h2: model.LocationHistory) -> model.Contact:
	users = {h1.id, h2.id}
	occurrences = _find_occurrences(h1, h2)
	return model.Contact(users=users, occurrences=occurrences)


def _find_occurrences(
		h1: model.LocationHistory,
		h2: model.LocationHistory) -> Set[model.Occurrence]:
	occurrences = set()
	if len(h1.history) == 0 and len(h2.history) == 0 or h1.id == h2.id:
		return occurrences
	iter1 = iter(sorted(h1.history))
	iter2 = iter(sorted(h2.history))
	loc1 = next(iter1, None)
	loc2 = next(iter2, None)
	started = False
	start = _get_later(loc1, loc2)
	while loc1 is not None and loc2 is not None:
		if loc1.location == loc2.location:
			if started:
				loc1 = next(iter1, None)
				loc2 = next(iter2, None)
			else:
				started = True
				start = _get_later(loc1, loc2)
		else:
			if started:
				started = False
				occurrence = _create_occurrence(start, loc1, loc2)
				if occurrence is not None:
					occurrences.add(occurrence)
			else:
				if loc1.timestamp < loc2.timestamp:
					loc1 = next(iter1, None)
				elif loc2.timestamp < loc2.timestamp:
					loc2 = next(iter2, None)
				else:
					if random.randint(0, 1) == 0:
						loc1 = next(iter1, None)
					else:
						loc2 = next(iter2, None)
	if started:
		occurrence = _create_occurrence(start, loc1, loc2)
		if occurrence is not None:
			occurrences.add(occurrence)
	return occurrences


def _create_occurrence(
		start: model.TemporalLocation,
		loc1: model.TemporalLocation,
		loc2: model.TemporalLocation):
	end = _get_earlier(loc1, loc2)
	duration = end.timestamp - start.timestamp
	if duration >= MIN_DURATION:
		occurrence = model.Occurrence(
			timestamp=start.timestamp, duration=duration)
	else:
		occurrence = None
	return occurrence


def _get_later(loc1: model.TemporalLocation, loc2: model.TemporalLocation):
	return loc1 if loc1.timestamp > loc2.timestamp else loc2


def _get_earlier(loc1: model.TemporalLocation, loc2: model.TemporalLocation):
	return loc1 if loc1.timestamp < loc2.timestamp else loc2

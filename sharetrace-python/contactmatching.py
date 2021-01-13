import datetime
import itertools
import random
from typing import Iterable, Optional

import codetiming as ct
import numpy as np
from ray.util import iter as it

import backend
import model

_MIN_DURATION = datetime.timedelta(minutes=15)
log = backend.LOGGER


def compute(
		locations: Iterable[model.LocationHistory],
		as_iterator: bool = True) -> Iterable[model.Contact]:
	with ct.Timer(text='CONTACT MATCHING: {:0.6f} s'):
		return _compute(locations, as_iterator)


def _compute(
		locations: Iterable[model.LocationHistory],
		as_iterator: bool = True) -> Iterable[model.Contact]:
	with ct.Timer(text='Creating unique pairs: {:0.6f} s', logger=log):
		pairs = list(itertools.combinations(locations, 2))
	with ct.Timer(text='Finding contacts: {:0.6f} s', logger=log):
		pairs = it.from_items(pairs, num_shards=backend.NUM_CPUS)
		contacts = pairs.for_each(
			lambda p: _find_contact(*p), max_concurrency=2)
		contacts = contacts.filter(lambda c: len(c.occurrences) > 0)
	with ct.Timer(text='Outputting contacts: {:0.6f} s', logger=log):
		if as_iterator:
			contacts = contacts.gather_async(num_async=backend.NUM_CPUS)
		else:
			contacts = np.array(list(contacts.gather_sync()))
		return contacts


def _find_contact(
		h1: model.LocationHistory, h2: model.LocationHistory) -> model.Contact:
	users = {h1.id, h2.id}
	occurrences = _find_occurrences(h1, h2)
	return model.Contact(users=users, occurrences=occurrences)


def _find_occurrences(
		h1: model.LocationHistory,
		h2: model.LocationHistory) -> Iterable[model.Occurrence]:
	occurrences = set()
	if len(h1.history) == 0 and len(h2.history) == 0 or h1.id == h2.id:
		return occurrences
	iter1 = iter(sorted(h1.history))
	iter2 = iter(sorted(h2.history))
	loc1 = next(iter1)
	loc2 = next(iter2)
	next1 = next(iter1, None)
	next2 = next(iter2, None)
	started = False
	start = _get_later(loc1, loc2)
	while next1 is not None and next2 is not None:
		if loc1.location == loc2.location:
			if started:
				loc1 = next1
				next1 = next(iter1, None)
				loc2 = next2
				next2 = next(iter2, None)
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
		occurrence = _create_occurrence(start, loc1, loc2)
		if occurrence is not None:
			occurrences.add(occurrence)
	return occurrences


def _create_occurrence(
		start: model.TemporalLocation,
		loc1: model.TemporalLocation,
		loc2: model.TemporalLocation) -> Optional[model.Occurrence]:
	end = _get_earlier(loc1, loc2)
	duration = end.timestamp - start.timestamp
	if duration >= _MIN_DURATION:
		occurrence = model.Occurrence(
			timestamp=start.timestamp, duration=duration)
	else:
		occurrence = None
	return occurrence


def _get_later(loc1: model.TemporalLocation, loc2: model.TemporalLocation):
	return loc1 if loc1.timestamp > loc2.timestamp else loc2


def _get_earlier(loc1: model.TemporalLocation, loc2: model.TemporalLocation):
	return loc1 if loc1.timestamp < loc2.timestamp else loc2

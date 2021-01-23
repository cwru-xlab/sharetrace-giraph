import datetime
import random

import codetiming
import numpy as np

import graphs
import model
import backend
import propagation
import search


def setup(
		*,
		users: int = 100,
		scores: int = 28,
		locations: int = 5,
		days: int = 14):
	users = max((1, users))
	locations = max((1, locations))
	days = max((1, days))
	start = datetime.datetime.today()
	factors = []
	variables = []
	for u in range(users):
		risks = [
			model.RiskScore(
				name=str(u),
				timestamp=start + datetime.timedelta(random.randint(0, days)),
				# Skewed toward lower scores
				value=random.betavariate(alpha=2, beta=5))
			for _ in range(scores)]
		variables.append((u, risks))
		history = (
			model.TemporalLocation(
				timestamp=start + datetime.timedelta(random.randint(0, days)),
				location=random.randint(0, locations))
			for _ in range(locations))
		factors.append(model.LocationHistory(name=u, history=history))
	return factors, variables


def simulate(
		*,
		factors,
		variables,
		transmission_rate: float = 0.8,
		iterations: int = 4,
		tolerance: float = 1e-5,
		impl=graphs.IGRAPH):
	transmission_rate = max((min((1, transmission_rate)), 0))
	iterations = max((1, iterations))
	tolerance = max(1e-16, tolerance)
	bp = propagation.BeliefPropagation(
		iterations=iterations,
		transmission_rate=transmission_rate,
		tolerance=tolerance,
		impl=impl)
	return bp(factors=factors, variables=variables)


def main():
	random.seed(12345)
	np.random.seed(12345)
	local_mode = False
	impl = graphs.NUMPY
	setup_kwargs = {'users': 200, 'scores': 60, 'days': 30}
	backend.set_local_mode(local_mode)
	contact_search = search.ContactSearch()
	if local_mode:
		factors, variables = setup(**setup_kwargs)
		factors = contact_search(factors)
		simulate(factors=factors, variables=variables, impl=impl)
	else:
		with backend.ray_context(num_cpus=backend.NUM_CPUS):
			factors, variables = setup(**setup_kwargs)
			factors = contact_search(factors)
			simulate(factors=factors, variables=variables, impl=impl)


if __name__ == '__main__':
	for i in range(1, 2):
		with codetiming.Timer(name='main'):
			main()
	print(f"mean:{codetiming.Timer.timers.mean('main')}")
	print(f"stdev:{codetiming.Timer.timers.stdev('main')}")
	print(f"max:{codetiming.Timer.timers.max('main')}")
	print(f"min:{codetiming.Timer.timers.min('main')}")

import datetime
import random

import codetiming as codetiming

import algorithm
import contactmatching
import model


def setup(
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
				id=u,
				timestamp=start + datetime.timedelta(random.randint(0, days)),
				value=random.random())
			for _ in range(scores)]
		variables.append((u, risks))
		locs = (
			model.LocationHistory(
				id=u,
				history=(
					model.TemporalLocation(
						timestamp=
						start + datetime.timedelta(random.randint(0, days)),
						location=random.randint(0, locations))
					for _ in range(locations))))
		factors.append(locs)
	return factors, variables


def simulate(
		factors,
		variables,
		transmission_rate: float = 0.8,
		iterations: int = 4,
		tolerance: float = 1e-5):
	transmission_rate = max((min((1, transmission_rate)), 0))
	iterations = max((1, iterations))
	tolerance = max(1e-16, tolerance)
	bp = algorithm.BeliefPropagation(
		iterations=iterations,
		transmission_rate=transmission_rate,
		tolerance=tolerance)
	return bp.run(factors=factors, variables=variables)


if __name__ == '__main__':
	with codetiming.Timer(text='Setup: {:0.4f} seconds'):
		factors, variables = setup(users=1000)
	with codetiming.Timer(text='Contact matching: {:0.4f} seconds'):
		factors = list(contactmatching.compute(factors))
	print(f'Number of contacts: {len(factors)}')
	with codetiming.Timer(text='Belief propagation: {:0.4f} seconds'):
		risks = simulate(factors, variables)

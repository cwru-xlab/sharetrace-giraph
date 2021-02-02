import datetime
import random

import numpy as np

import backend
import graphs
import model
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
				timestamp=start - datetime.timedelta(random.randint(0, days)),
				# Skewed toward lower scores
				value=generate_bimodal_score())
			for _ in range(scores)]
		variables.append((u, risks))
		history = (
			model.TemporalLocation(
				timestamp=start - datetime.timedelta(random.randint(0, days)),
				location=random.randint(0, locations))
			for _ in range(locations))
		factors.append(model.LocationHistory(name=u, history=history))
	return factors, variables


def generate_bimodal_score(mean1=0.2, var1=0.01, w1=0.95, mean2=0.8, var2=0.01):
	x1 = random.normalvariate(mean1, np.sqrt(var1))
	x2 = random.normalvariate(mean2, np.sqrt(var2))
	score = random.choices([x1, x2], weights=[w1, 1 - w1])[0]
	return np.clip(score, 0, 1)


def simulate(
		*,
		factors,
		variables,
		transmission_rate: float = 0.8,
		iterations: int = 4,
		tolerance: float = 1e-5,
		impl=graphs.NUMPY,
		local_mode: bool = True):
	transmission_rate = max(min(1., transmission_rate), 0.)
	iterations = max(1, iterations)
	tolerance = max(1e-6, tolerance)
	kwargs = {
		'iterations': iterations,
		'transmission_rate': transmission_rate,
		'tolerance': tolerance,
		'impl': impl}
	if local_mode:
		bp = propagation.LocalBeliefPropagation(**kwargs)
	else:
		bp = propagation.RemoteBeliefPropagation(**kwargs)
	return bp(factors=factors, variables=variables)


def main():
	random.seed(12345)
	np.random.seed(12345)
	local_mode = False
	impl = graphs.NUMPY
	setup_kwargs = {'users': 100, 'scores': 14, 'days': 14, 'locations': 10}
	iterations = 700
	contact_search = search.ContactSearch()
	if local_mode:
		factors, variables = setup(**setup_kwargs)
		factors = contact_search(factors)
		result = simulate(
			factors=factors,
			variables=variables,
			iterations=iterations,
			transmission_rate=0.8,
			impl=impl,
			local_mode=local_mode)
	else:
		with backend.ray_context(num_cpus=backend.NUM_CPUS):
			factors, variables = setup(**setup_kwargs)
			factors = contact_search(factors)
			result = simulate(
				factors=factors,
				variables=variables,
				iterations=iterations,
				impl=impl,
				local_mode=local_mode)
	print('-------------------------------------')
	for r in result:
		print(r)


if __name__ == '__main__':
	main()

import datetime
import random

import numpy as np

import backend
import graphs
import model
import propagation
import search


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


def main(print_result: bool = False):
	local_mode = True
	bp_kwargs = {
		'iterations': 4,
		'transmission_rate': 0.8,
		'tolerance': 1e-6,
		'impl': graphs.NUMPY,
		'send_condition': 'local',
		'send_threshold': 0.75,
		'seed': None}
	factors, variables = setup(users=100, scores=14, days=14, locations=10)
	if local_mode:
		contact_search = search.ContactSearch()
		factors = contact_search(factors)
		bp = propagation.LocalBeliefPropagation(**bp_kwargs)
		result = bp(factors=factors, variables=variables)
	else:
		with backend.ray_context():
			contact_search = search.ContactSearch()
			factors = contact_search(factors)
			bp = propagation.RemoteBeliefPropagation(**bp_kwargs)
			result = bp(factors=factors, variables=variables)
	if print_result:
		print('-------------------------------------')
		for r in result:
			print(r)


if __name__ == '__main__':
	main(print_result=True)

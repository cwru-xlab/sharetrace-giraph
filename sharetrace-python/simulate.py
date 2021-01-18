import asyncio
import datetime
import random

import algorithm
import backend
import contactmatching
import graphs
import model


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
				value=random.random())
			for _ in range(scores)]
		variables.append((u, risks))
		history = (
			model.TemporalLocation(
				timestamp=start + datetime.timedelta(random.randint(0, days)),
				location=random.randint(0, locations))
			for _ in range(locations))
		factors.append(model.LocationHistory(name=u, history=history))
	return factors, variables


async def simulate(
		*,
		factors,
		variables,
		transmission_rate: float = 0.8,
		iterations: int = 4,
		tolerance: float = 1e-5,
		impl=graphs.IGRAPH,
		local_mode=True):
	transmission_rate = max((min((1, transmission_rate)), 0))
	iterations = max((1, iterations))
	tolerance = max(1e-16, tolerance)
	backend.LOCAL_MODE = local_mode
	bp = algorithm.BeliefPropagation(
		iterations=iterations,
		transmission_rate=transmission_rate,
		tolerance=tolerance,
		backend=impl,
		local_mode=local_mode)
	return await bp(factors=factors, variables=variables)


if __name__ == '__main__':
	local_mode = True
	impl = graphs.IGRAPH
	users = 50
	if local_mode:
		factors, variables = setup(users=users)
		factors = contactmatching.compute(factors)
		risks = asyncio.run(simulate(
			factors=factors,
			variables=variables,
			impl=impl,
			local_mode=local_mode))
	else:
		with backend.ray_context(num_cpus=backend.NUM_CPUS):
			factors, variables = setup(users=users)
			factors = contactmatching.compute(factors)
			risks = asyncio.run(simulate(
				factors=factors,
				variables=variables,
				impl=impl,
				local_mode=local_mode))

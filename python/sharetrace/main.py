import asyncio
from typing import Hashable, Iterable, Tuple

import algorithm
import backend
import contactmatching
import model
import pda


async def main():
	backend.set_local_mode(True)
	async with pda.PdaContext() as p:
		token, hats = await p.get_token_and_hats()
		variables, locations = await asyncio.gather(
			p.get_scores(token=token, hats=hats),
			p.get_locations(token=token, hats=hats))
	if backend.LOCAL_MODE:
		updated_scores = _compute(locations, variables)
	else:
		with backend.ray_context():
			updated_scores = _compute(locations, variables)
	async with pda.PdaContext() as p:
		await p.post_scores(scores=updated_scores, token=token)


def _compute(
		locations: Iterable[model.LocationHistory],
		variables: algorithm.GroupedRiskScores
) -> Iterable[Tuple[Hashable, model.RiskScore]]:
	factors = contactmatching.compute(locations)
	bp = algorithm.BeliefPropagation()
	return bp(factors=factors, variables=variables)


if __name__ == '__main__':
	asyncio.run(main())

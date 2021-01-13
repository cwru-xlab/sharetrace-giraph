import asyncio

import algorithm
import backend
import contactmatching
import pda


async def main():
	async with pda.PdaContext() as p:
		token, hats = await p.get_token_and_hats()
		variables, locations = await asyncio.gather(
			p.get_scores(token=token, hats=hats),
			p.get_locations(token=token, hats=hats))
	with backend.ray_context():
		factors = contactmatching.compute(locations)
		bp = algorithm.BeliefPropagation()
		updated_scores = bp(factors=factors, variables=variables)
	async with pda.PdaContext() as p:
		await p.post_scores(scores=updated_scores, token=token)


if __name__ == '__main__':
	asyncio.run(main())

import asyncio

import algorithm
import backend
import contactmatching
import pda


def main():
	with pda.PdaContext() as p:
		token, hats = await p.get_token_and_hats()
		variables, locations = await asyncio.gather(
			p.get_scores(hats=hats, token=token),
			p.get_locations(hats=hats, token=token))
	with backend.ray_context():
		factors = contactmatching.compute(locations)
		bp = algorithm.BeliefPropagation()
		updated_scores = bp(factors=factors, variables=variables)
	with pda.PdaContext() as p:
		responses = await p.post_scores(scores=updated_scores, token=token)


if __name__ == '__main__':
	main()

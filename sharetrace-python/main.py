import algorithm
import contactmatching
import pda

if __name__ == '__main__':
	token, hats = pda.get_token_and_hats()
	locations = pda.get_locations(hats=hats, token=token)
	locations = pda.map_to_locations(locations)
	factors = contactmatching.compute(locations)
	scores = pda.get_scores(hats=hats, token=token)
	variables = pda.map_to_scores(scores)
	bp = algorithm.BeliefPropagation(factors=factors, variables=variables)
	updated_scores = bp.run()
	pda.post_scores(scores=updated_scores, token=token)

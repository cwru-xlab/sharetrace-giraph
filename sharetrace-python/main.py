import algorithm
import contactmatching
import pda


def main():
	token, hats = pda.get_token_and_hats()
	locations = pda.get_locations(hats=hats, token=token)
	locations = pda.map_to_locations(locations)
	locations = (history for _, history in locations)
	factors = contactmatching.compute(locations)
	scores = pda.get_scores(hats=hats, token=token)
	variables = pda.map_to_scores(scores)
	bp = algorithm.BeliefPropagation()
	updated_scores = bp.run(factors=factors, variables=variables)
	pda.post_scores(scores=updated_scores, token=token)


if __name__ == '__main__':
	main()

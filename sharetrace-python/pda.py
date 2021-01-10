import datetime
import json
import time
from typing import Any, Collection, Iterable, Mapping, Tuple

import requests

import model

"""
	Assumed HAT API response schema:
	response = {
		hat: {
			scores: {
				data: {
					score: <score>,
					timestamp: <timestamp>
				}
			}
			locations: [
				data: {
					hash: <hash>,
					timestamp: <timestamp>
				}
			]
		}
	}
	"""

CLIENT_NAMESPACE = '/emitto'
READ_SCORE_NAMESPACE = '/healthsurveyscores'
CREATE_SCORE_NAMESPACE = '/exposurescore'
LOCATION_NAMESPACE = '/locationvectors'
CONTRACT_ID = '744c7472-3fe5-4492-8eda-5d387d6686d9'
CONTENT_TYPE_HEADER = {'Content-Type': 'application/json'}
AUTH = (
	'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJBcHBsaWNhdGlvbkRl'
	'dmVsb3BlciIsCiAgImNvbnRyYWN0SWQiIDogIjc0NGM3NDcyLTNmZTUtNDQ5Mi04ZWRhLTVk'
	'Mzg3ZDY2ODZkOSIsCiAgImRldmVsb3BlcklkIiA6ICJ1ay0yMDI3MzItZ3IiCn0.yLr-oJFq'
	'Ew7byMQpeT2wTU5olE1-8H5wOlZek4JXFmo ')
AUTH_HEADER = {'Authorization': AUTH}
KEYRING_URL = 'https://contracts.dataswift.io/v1/contracts/keyring'
READ_URL = 'https://{}.hubofallthings.net/api/v2.6/contract-data/read'
CREATE_URL = 'https://{}.hubofallthings.net/api/v2.6/contract-data/create'
SUCCESS_CODE = 200
ORDER_BY = 'timestamp'
ORDERING = 'ascending'
BASE_READ_BODY = {
	'contractId': CONTRACT_ID,
	'orderBy': ORDER_BY,
	'ordering': ORDERING,
	'skip': 0}
TWO_WEEKS_AGO = datetime.datetime.today() - datetime.timedelta(days=14)


def map_to_scores(
		response: Iterable[Tuple[str, Mapping[str, Any]]],
		as_generator: bool = True) -> Iterable:
	def to_scores(hat: str, entry: Mapping[str, Any]):
		risk_scores = (r['data'] for r in entry['scores'])
		risk_scores = ((
			r['score'] / 100, _to_timestamp(r['timestamp'])
			for r in risk_scores))
		risk_scores = (
			model.RiskScore(id=hat, timestamp=t, value=v)
			for v, t in risk_scores)
		if not as_generator:
			return frozenset(risk_scores)

	scores = ((h, to_scores(h, data)) for h, data in response)
	if not as_generator:
		scores = {h: h_scores for h, h_scores in scores}
	return scores


def _to_timestamp(ms_timestamp: float) -> datetime.datetime:
	return datetime.datetime.utcfromtimestamp(ms_timestamp / 1000)


def map_to_locations(
		response: Iterable[Tuple[str, Any]],
		since: datetime.datetime = TWO_WEEKS_AGO,
		hash_obfuscation: int = 3,
		as_generator: bool = True) -> Iterable:
	def to_history(hat: str, entry: Mapping[str, Any]):
		locations = (loc['data'] for loc in entry['locations'])
		locations = ((
			_to_timestamp(loc['timestamp']), loc['hash'][:-hash_obfuscation])
			for loc in locations)
		locations = (
			model.TemporalLocation(timestamp=t, location=h)
			for t, h in locations if t >= since)
		return model.LocationHistory(id=hat, history=locations)

	histories = (h, to_history(h, data) for h, data in response)
	if not as_generator:
		histories = {h: history for h, history in histories}
	return histories


def get_token_and_hats() -> Tuple[str, Collection[str]]:
	response = requests.get(KEYRING_URL, headers=AUTH_HEADER)
	status_code = response.status_code
	text = response.text
	if status_code != SUCCESS_CODE:
		raise IOError(f'{status_code}: Unable to authorize keyring )\n{text}')
	response = json.loads(response.text)
	return response['token'], response['associatedHats']


def get_scores(
		hats: Iterable[str],
		token: str,
		as_generator: bool = True) -> Iterable:
	namespace = ''.join((CLIENT_NAMESPACE, READ_SCORE_NAMESPACE))
	scores = (h, json.loads(_get_data(h, token, namespace).text) for h in hats)
	if not as_generator:
		scores = {h: h_scores for h, h_scores in scores}
	return scores


def get_locations(
		hats: Iterable[str],
		token: str,
		as_generator: bool = True) -> Iterable:
	namespace = ''.join((CLIENT_NAMESPACE, LOCATION_NAMESPACE))
	responses = (h, _get_data(h, token, namespace).text for h in hats)
	locations = (h, json.loads(response) for h, response in responses)
	if not as_generator:
		locations = {h: h_locations for h, h_locations in locations}
	return locations


def _get_data(hat: str, token: str, namespace: str) -> requests.Response:
	body = BASE_READ_BODY.copy()
	body.update({'token': token, 'hatName': hat, 'skip': 0})
	url = ''.join((READ_URL.format(hat), namespace))
	return requests.post(url, json=body, headers=CONTENT_TYPE_HEADER)


def post_scores(
		scores: Iterable[model.RiskScore],
		token: str) -> Iterable[requests.Response]:
	timestamp = time.time() * 1000
	namespace = ''.join((CLIENT_NAMESPACE, CREATE_SCORE_NAMESPACE))
	responses = []
	for score in scores:
		hat = score.id
		value = round(score.value * 100, 2)
		body = {
			'token': token,
			'contractId': CONTRACT_ID,
			'hatName': hat,
			'body': {'score': value, 'timestamp': timestamp}}
		url = ''.join((CREATE_URL.format(hat), namespace))
		response = requests.post(url, json=body, headers=CONTENT_TYPE_HEADER)
		responses.append(response)
	return responses

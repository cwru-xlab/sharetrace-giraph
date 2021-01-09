import datetime
import json
import time
from typing import Any, Dict, Iterable, Sequence, Tuple

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


def map_to_scores(response: Dict) -> Sequence[model.RiskScore]:
	# TODO Are multiple risk scores per hat returned?
	def to_score(hat: str):
		data = response[hat]['scores']['data']
		score = data['score'] / 100
		timestamp = datetime.datetime.utcfromtimestamp(
			data['timestamp'] / 1000)
		return model.RiskScore(id=hat, timestamp=timestamp, value=score)

	return tuple(to_score(h) for h in response)


def map_to_locations(
		response: Dict,
		since: datetime.datetime = TWO_WEEKS_AGO,
		hash_obfuscation: int = 3,
) -> Sequence[model.LocationHistory]:
	def to_history(hat: str):
		data = response[hat]['locations']['data']
		locations = (
			datetime.datetime.utcfromtimestamp(loc['timestamp'] / 1000),
			loc['hash'][:-hash_obfuscation]
			for loc in data)
		locations = (
			model.TemporalLocation(timestamp=t, location=h)
			for t, h in locations if t >= since)
		return model.LocationHistory(id=hat, history=tuple(locations))

	return tuple(to_history(h) for h in response)


def get_token_and_hats() -> Tuple[str, Sequence[str]]:
	response = requests.get(KEYRING_URL, headers=AUTH_HEADER)
	status_code = response.status_code
	text = response.text
	if status_code != SUCCESS_CODE:
		raise IOError(
			(f'Unable to authorize keyring (status code: {status_code})\
			n{text}'))
	response = json.loads(response.text)
	return response['token'], response['associatedHats']


def get_scores(hats: Iterable[str], token: str) -> Dict[str, Any]:
	namespace = ''.join((CLIENT_NAMESPACE, READ_SCORE_NAMESPACE))
	return {h: json.loads(_get_data(h, token, namespace).text) for h in hats}


def get_locations(hats: Iterable[str], token: str) -> Dict[str, Any]:
	namespace = ''.join((CLIENT_NAMESPACE, LOCATION_NAMESPACE))
	return {h: json.loads(_get_data(h, token, namespace).text) for h in hats}


def _get_data(hat: str, token: str, namespace: str):
	body = BASE_READ_BODY.copy()
	body.update({'token': token, 'hatName': hat, 'skip': 0})
	url = ''.join((READ_URL.format(hat), namespace))
	return requests.post(url, json=body, headers=CONTENT_TYPE_HEADER)


def post_scores(scores: Iterable[model.RiskScore], token: str):
	timestamp = time.time() * 1000
	namespace = ''.join((CLIENT_NAMESPACE, CREATE_SCORE_NAMESPACE))
	for score in scores:
		hat = score.id
		value = round(score.value * 100, 2)
		body = {
			'token': token,
			'contractId': CONTRACT_ID,
			'hatName': hat,
			'body': {'score': value, 'timestamp': timestamp}}
		url = ''.join((CREATE_URL.format(hat), namespace))
		requests.post(url, json=body, headers=CONTENT_TYPE_HEADER)

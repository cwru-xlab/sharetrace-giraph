import asyncio
import datetime
import functools
import json
import time
from typing import Any, Iterable, Mapping, NoReturn, Tuple

import aiohttp
import attr
import numpy as np

import model

"""
	Assumed HAT API data schema:
	data = {
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
TWO_WEEKS_AGO = datetime.datetime.utcnow() - datetime.timedelta(days=14)


@attr.s(slots=True)
class PdaContext:
	_session = attr.ib(type=aiohttp.ClientSession, init=False)

	async def __aenter__(self):
		self._session = aiohttp.ClientSession()
		return self

	async def __aexit__(self, exc_type, exc_val, exc_tb):
		await self._session.close()

	async def get_token_and_hats(self) -> Tuple[str, Iterable[str]]:
		async with self._session.get(KEYRING_URL, headers=AUTH_HEADER) as r:
			status = r.status
			text = await r.text()
			text = json.loads(text)
			if status != SUCCESS_CODE:
				msg = f'{status}: unable to authorize keyring\n{text}'
				raise IOError(msg)
		return text['token'], np.array(text['associatedHats'])

	async def get_scores(
			self,
			token: str,
			hats: Iterable[str],
			since: datetime.datetime = TWO_WEEKS_AGO
	) -> Iterable[model.RiskScore]:
		namespace = ''.join((CLIENT_NAMESPACE, READ_SCORE_NAMESPACE))
		get_data = functools.partial(
			self._get_data, token=token, namespace=namespace)
		return await asyncio.gather(
			(h, self._to_scores(hat=h, data=await get_data(h), since=since))
			for h in hats)

	def _to_scores(
			self,
			hat: str,
			data: Mapping[str, Any],
			since: datetime.datetime = TWO_WEEKS_AGO
	) -> Tuple[str, Iterable[model.RiskScore]]:
		values = (s['data'] for s in data['scores'])
		values = ((s['score'] / 1e2, s['timestamp'] / 1e3) for s in values)
		scores = (
			model.RiskScore(id=hat, timestamp=t, value=v)
			for t, v in values if t >= since)
		return hat, scores

	async def get_locations(
			self,
			token: str,
			hats: Iterable[str],
			since: datetime.datetime = TWO_WEEKS_AGO,
			hash_obfuscation: int = 3) -> Iterable[model.LocationHistory]:
		namespace = ''.join((CLIENT_NAMESPACE, LOCATION_NAMESPACE))
		get_data = functools.partial(
			self._get_data, namespace=namespace, token=token)
		return await asyncio.gather(
			self._to_locations(
				hat=h,
				data=await get_data(hat=h),
				since=since,
				hash_obfuscation=hash_obfuscation)
			for h in hats)

	async def _to_locations(
			self,
			hat: str,
			data: Mapping[str, Any],
			since: datetime.datetime = TWO_WEEKS_AGO,
			hash_obfuscation: int = 3) -> model.LocationHistory:
		locs = (loc['data'] for loc in data['locations'])
		locs = (
			(loc['timestamp'] / 1e3, loc['hash'][:-hash_obfuscation])
			for loc in locs)
		locs = (
			model.TemporalLocation(timestamp=t, location=h)
			for t, h in locs if t >= since)
		return model.LocationHistory(id=hat, history=locs)

	async def _get_data(
			self, token: str, hat: str, namespace: str) -> Mapping[str, Any]:
		body = BASE_READ_BODY.copy()
		body.update({'token': token, 'hatName': hat, 'skip': 0})
		url = ''.join((READ_URL.format(hat), namespace))
		async with self._session.post(
				url, json=body, headers=CONTENT_TYPE_HEADER) as r:
			text = await r.text()
			text = json.loads(text)
			status = r.status
			if status != SUCCESS_CODE:
				msg = f'{status}: failed to get data from {hat}\n{text}'
				raise IOError(msg)
			return text

	async def post_scores(
			self,
			token: str,
			scores: Iterable[Tuple[str, model.RiskScore]]) -> NoReturn:
		namespace = ''.join((CLIENT_NAMESPACE, CREATE_SCORE_NAMESPACE))
		timestamp = time.time() * 1e3

		async def post(hat: str, score: model.RiskScore):
			value = round(score.value * 1e2, 2)
			body = {
				'token': token,
				'contractId': CONTRACT_ID,
				'hatName': hat,
				'body': {'score': value, 'timestamp': timestamp}}
			url = ''.join((CREATE_URL.format(hat), namespace))
			async with self._session.post(
					url, json=body, headers=CONTENT_TYPE_HEADER) as r:
				text = await r.text()
				text = json.loads(text)
				status = r.status
				if status != SUCCESS_CODE:
					msg = f'{status}: failed to send data to {hat}\n{text}'
					raise IOError(msg)

		await asyncio.gather((h, post(h, s)) for h, s in scores)

import asyncio
import datetime
import functools
import json
import time
from typing import Any, Iterable, Mapping, NoReturn, Optional, Tuple

import aiohttp
import attr
import numpy as np

import model

"""
Assumed HAT API data schema (for a single HAT):
	scores = [{data: {score: <score>, timestamp: <timestamp>}} ...]
	locations = [{data: {hash: <hash>, timestamp: <timestamp>}} ...]
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
ORDERING = 'descending'
BASE_READ_BODY = {
	'contractId': CONTRACT_ID,
	'orderBy': ORDER_BY,
	'ordering': ORDERING,
	'skip': 0,
	'take': 100}
TWO_WEEKS_AGO = datetime.datetime.utcnow() - datetime.timedelta(days=14)
Response = Mapping[str, Any]


@attr.s(slots=True)
class PdaContext:
	_session = attr.ib(type=aiohttp.ClientSession, init=False)

	async def __aenter__(self):
		self._session = aiohttp.ClientSession()
		return self

	async def __aexit__(self, exc_type, exc_val, exc_tb):
		await self._session.close()

	async def get_token_and_hats(self) -> Tuple[str, Iterable[str]]:
		async with self._session.get(
				KEYRING_URL, headers=AUTH_HEADER) as r:
			text = await self._handle_response(r)
			return text['token'], np.array(text['associatedHats'])

	async def get_scores(
			self,
			*,
			token: str,
			hats: Iterable[str],
			since: datetime.datetime = TWO_WEEKS_AGO
	) -> Iterable[model.RiskScore]:
		namespace = ''.join((CLIENT_NAMESPACE, READ_SCORE_NAMESPACE))
		data = await asyncio.gather(*[
			self._get_data(token=token, namespace=namespace, hat=h)
			for h in hats])
		return await asyncio.gather(*[
			self._to_scores(h, d, since) for h, d in zip(hats, data)])

	@staticmethod
	async def _to_scores(
			hat: str,
			data: Iterable[Mapping[str, Any]],
			since: datetime.datetime = TWO_WEEKS_AGO
	) -> Tuple[str, Iterable[model.RiskScore]]:
		values = (s['data'] for s in data)
		values = ((s['score'], _to_timestamp(s['timestamp'])) for s in values)
		scores = (
			model.RiskScore(name=hat, value=v, timestamp=t)
			for t, v in values if t >= since)
		return hat, scores

	async def get_locations(
			self,
			*,
			token: str,
			hats: Iterable[str],
			since: datetime.datetime = TWO_WEEKS_AGO,
			obfuscation: int = 3) -> Iterable[model.LocationHistory]:
		namespace = ''.join((CLIENT_NAMESPACE, LOCATION_NAMESPACE))
		get_data = functools.partial(
			self._get_data, namespace=namespace, token=token)
		return await asyncio.gather(*[
			self._to_locations(h, await get_data(hat=h), since, obfuscation)
			for h in hats])

	@staticmethod
	async def _to_locations(
			hat: str,
			data: Iterable[Mapping[str, Any]],
			since: datetime.datetime = TWO_WEEKS_AGO,
			obfuscation: int = 3) -> model.LocationHistory:
		locs = (loc['data'] for loc in data)
		locs = (
			(_to_timestamp(loc['timestamp']), loc['hash'][:-obfuscation])
			for loc in locs)
		locs = (
			model.TemporalLocation(timestamp=t, location=h)
			for t, h in locs if t >= since)
		return model.LocationHistory(name=hat, history=locs)

	async def _get_data(
			self, token: str, hat: str, namespace: str) -> Response:
		body = BASE_READ_BODY.copy()
		body.update({'token': token, 'hatName': hat, 'skip': 0})
		url = ''.join((READ_URL.format(hat), namespace))
		async with self._session.post(
				url, json=body, headers=CONTENT_TYPE_HEADER) as r:
			return await self._handle_response(r, hat=hat, send=False)

	async def post_scores(
			self,
			token: str,
			scores: Iterable[Tuple[str, model.RiskScore]]) -> NoReturn:
		namespace = ''.join((CLIENT_NAMESPACE, CREATE_SCORE_NAMESPACE))
		timestamp = time.time() * 1e3

		async def post(hat: str, score: model.RiskScore):
			value = round(score.value, 2)
			body = {
				'token': token,
				'contractId': CONTRACT_ID,
				'hatName': hat,
				'body': {'score': value, 'timestamp': timestamp}}
			url = ''.join((CREATE_URL.format(hat), namespace))
			async with self._session.post(
					url, json=body, headers=CONTENT_TYPE_HEADER) as r:
				await self._handle_response(r, hat=hat)

		await asyncio.gather(*[post(h, s) for h, s in scores])

	@staticmethod
	async def _handle_response(
			response: aiohttp.ClientResponse,
			hat: Optional[str] = None,
			send: Optional[bool] = True) -> Response:
		def check_hat():
			if send is not None and hat is None:
				raise ValueError(
					'Must provide HAT name to handle non-keyring response')

		text = await response.text()
		text = json.loads(text)
		status = response.status
		if status != SUCCESS_CODE:
			if send:
				check_hat()
				msg = f'{status}: failed to send data to {hat}\n{text}'
			elif send is False:
				check_hat()
				msg = f'{status}: failed to get data from {hat}\n{text}'
			else:
				msg = f'{status}: failed to authorize keyring\n{text}'
			raise IOError(msg)
		return text


def _to_timestamp(ms_timestamp: float):
	return datetime.datetime.utcfromtimestamp(ms_timestamp / 1e3)

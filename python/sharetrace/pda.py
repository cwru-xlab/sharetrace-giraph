import asyncio
import datetime
import functools
import json
import time
from typing import Any, Iterable, Mapping, NoReturn, Optional, Tuple, Union

import aiohttp
import attr
import codetiming
import numpy as np

import backend
import model

"""
Assumed HAT API data schema (for a single HAT):
	scores = [{data: {score: <score>, timestamp: <timestamp>}} ...]
	histories = [{data: {hash: <hash>, timestamp: <timestamp>}} ...]
"""

CONTENT_TYPE_HEADER = {'Content-Type': 'application/json'}
SUCCESS_CODE = 200
HTTPS = 'https://'
BASE_READ_BODY = {'orderBy': 'timestamp', 'ordering': 'descending', 'skip': 0}
TWO_WEEKS_AGO = datetime.datetime.utcnow() - datetime.timedelta(days=14)
Response = Union[Mapping[str, Any], Iterable[Mapping[str, Any]]]
READ_HATS_MSG = 'Reading tokens and hats: {:0.6f} s'
READ_SCORES_MSG = 'Reading scores: {:0.6f} s'
READ_LOCATIONS_MSG = 'Reading location histories: {:0.6f} s'
WRITE_SCORES_MSG = 'Writing scores: {:0.6f} s'
stdout = backend.STDOUT
stderr = backend.STDERR


@attr.s(slots=True)
class PdaContext:
	client_namespace = attr.ib(type=str, default=None)
	contract_id = attr.ib(type=str, default=None)
	keyring_url = attr.ib(type=str, default=None)
	read_url = attr.ib(type=str, default=None)
	write_url = attr.ib(type=str, default=None)
	long_lived_token = attr.ib(type=str, default=None)
	_session = attr.ib(type=aiohttp.ClientSession, init=False, repr=False)

	async def __aenter__(self):
		self._session = aiohttp.ClientSession()
		return self

	async def __aexit__(self, exc_type, exc_val, exc_tb):
		await self._session.close()

	@codetiming.Timer(text=READ_HATS_MSG, logger=stdout)
	async def get_token_and_hats(self) -> Tuple[str, Iterable[str]]:
		headers = {'Authorization': f'Bearer {self.long_lived_token}'}
		async with self._session.get(self.keyring_url, headers=headers) as r:
			text = await self._handle_response(r)
		hats = np.array(text['associatedHats'])
		stdout(f'Number of hats retrieved: {len(hats)}')
		return text['token'], hats

	@codetiming.Timer(text=READ_SCORES_MSG, logger=stdout)
	async def get_scores(
			self,
			token: str,
			*,
			hats: Iterable[str],
			score_namespace: str,
			take: int = None,
			since: datetime.datetime = TWO_WEEKS_AGO
	) -> Iterable[model.RiskScore]:
		namespace = '/'.join((self.client_namespace, score_namespace))
		data = await asyncio.gather(*[
			self._get_data(token=token, namespace=namespace, hat=h, take=take)
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

	@codetiming.Timer(text=READ_LOCATIONS_MSG, logger=logger)
	async def get_locations(
			self,
			token: str,
			*,
			hats: Iterable[str],
			location_namespace: str,
			take: int = None,
			since: datetime.datetime = TWO_WEEKS_AGO,
			obfuscation: int = 3) -> Iterable[model.LocationHistory]:
		namespace = '/'.join((self.client_namespace, location_namespace))
		get_data = functools.partial(
			self._get_data, namespace=namespace, token=token, take=take)
		return await asyncio.gather(*[
			self._to_locations(h, await get_data(hat=h), since, obfuscation)
			for h in hats])

	@staticmethod
	async def _to_locations(
			hat: str,
			data: Iterable[Mapping[str, Any]],
			since: datetime.datetime,
			obfuscation: int) -> model.LocationHistory:
		locs = (loc['data'] for loc in data)
		locs = (
			(_to_timestamp(loc['timestamp']), loc['hash'][:-obfuscation])
			for loc in locs)
		locs = (
			model.TemporalLocation(timestamp=t, location=h)
			for t, h in locs if t >= since)
		return model.LocationHistory(name=hat, history=locs)

	async def _get_data(
			self,
			token: str,
			hat: str,
			namespace: str,
			take: int) -> Response:
		body = BASE_READ_BODY.copy()
		body.update({
			'token': token,
			'hatName': hat,
			'contractId': self.contract_id})
		if take:
			body['take'] = take
		url = _format_url(self.read_url, hat, namespace)
		async with self._session.post(
				url, json=body, headers=CONTENT_TYPE_HEADER) as r:
			return await self._handle_response(r, hat=hat, send=False)

	@codetiming.Timer(text=WRITE_SCORES_MSG, logger=stdout)
	async def post_scores(
			self,
			token: str,
			*,
			scores: Iterable[Tuple[str, model.RiskScore]],
			namespace: str) -> NoReturn:
		namespace = '/'.join((self.client_namespace, namespace))
		timestamp = time.time() * 1e3

		async def post(hat: str, score: model.RiskScore):
			value = round(score.value, 2)
			body = {
				'token': token,
				'contractId': self.contract_id,
				'hatName': hat,
				'body': {'score': value, 'timestamp': timestamp}}
			url = _format_url(self.write_url, hat, namespace)
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
			stderr(msg)
		return text


def _format_url(base_url: str, hat: str, namespace: str):
	with_hat = ''.join((HTTPS, f'{hat}.', base_url.split(HTTPS)[-1]))
	return '/'.join((with_hat, namespace))


def _to_timestamp(ms_timestamp: float):
	return datetime.datetime.utcfromtimestamp(ms_timestamp / 1e3)

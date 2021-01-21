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

import _model
import backend

SUCCESS_CODE = 200
_CONTENT_TYPE_HEADER = {'Content-Type': 'application/json'}
_HTTPS = 'https://'
_BASE_READ_BODY = {'orderBy': 'timestamp', 'ordering': 'descending', 'skip': 0}
_TWO_WEEKS_AGO = datetime.datetime.utcnow() - datetime.timedelta(days=14)
_Response = Union[Mapping[str, Any], Iterable[Mapping[str, Any]]]
_READ_HATS_MSG = 'Reading tokens and hats: {:0.6f} s'
_READ_SCORES_MSG = 'Reading scores: {:0.6f} s'
_READ_LOCATIONS_MSG = 'Reading location histories: {:0.6f} s'
_WRITE_SCORES_MSG = 'Writing scores: {:0.6f} s'
stdout = backend.STDOUT
stderr = backend.STDERR


@attr.s(slots=True)
class PdaContext:
	"""Contracted PDA context manager for the ShareTrace project.

	Communicates with user PDAs to retrieve data to send back computed
	exposure scores.

	Attributes:
		client_namespace: Endpoint containing the contracted namespaces.
		contract_id: A unique value that identifies the contract.
		keyring_url: Endpoint from which to retrieve a short-lived token.
		read_url: Endpoint from which to read contracted PDA data.
		write_url: Endpoint from which to write contracted PDA data.
		long_lived_token: Required to retrieve a short-lived token and send
			requests.
	"""
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

	@codetiming.Timer(text=_READ_HATS_MSG, logger=stdout)
	async def get_token_and_hats(self) -> Tuple[str, Iterable[str]]:
		"""Retrieves a short-lived token and contracted-associated HATs."""
		headers = {'Authorization': f'Bearer {self.long_lived_token}'}
		async with self._session.get(self.keyring_url, headers=headers) as r:
			text = await self._handle_response(r)
		hats = np.array(text['associatedHats'])
		stdout(f'Number of hats retrieved: {len(hats)}')
		return text['token'], hats

	@codetiming.Timer(text=_READ_SCORES_MSG, logger=stdout)
	async def get_scores(
			self,
			token: str,
			*,
			hats: Iterable[str],
			score_namespace: str,
			take: int = None,
			since: datetime.datetime = _TWO_WEEKS_AGO
	) -> Iterable[Tuple[str, Iterable[_model.RiskScore]]]:
		"""Retrieves the survey risk scores from the PDAs.

		Each response record to is mapped to a collection of RiskScore objects.
		RiskScore objects are grouped by HAT.
		"""
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
			since: datetime.datetime = _TWO_WEEKS_AGO
	) -> Tuple[str, Iterable[_model.RiskScore]]:
		values = (s['data'] for s in data)
		values = ((s['score'], _to_timestamp(s['timestamp'])) for s in values)
		scores = (
			_model.RiskScore(name=hat, value=v, timestamp=t)
			for t, v in values if t >= since)
		return hat, scores

	@codetiming.Timer(text=_READ_LOCATIONS_MSG, logger=stdout)
	async def get_locations(
			self,
			token: str,
			*,
			hats: Iterable[str],
			location_namespace: str,
			take: int = None,
			since: datetime.datetime = _TWO_WEEKS_AGO,
			obfuscation: int = 3) -> Iterable[_model.LocationHistory]:
		"""Retrieves the location data from the PDAs.

		Maps each response record to a LocationHistory object.
		"""
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
			obfuscation: int) -> _model.LocationHistory:
		locs = (loc['data'] for loc in data)
		locs = (
			(_to_timestamp(loc['timestamp']), loc['hash'][:-obfuscation])
			for loc in locs)
		locs = (
			_model.TemporalLocation(timestamp=t, location=h)
			for t, h in locs if t >= since)
		return _model.LocationHistory(name=hat, history=locs)

	async def _get_data(
			self,
			token: str,
			hat: str,
			namespace: str,
			take: int) -> _Response:
		body = _BASE_READ_BODY.copy()
		body.update({
			'token': token,
			'hatName': hat,
			'contractId': self.contract_id})
		if take is not None:
			body['take'] = take
		url = self._format_url(self.read_url, hat, namespace)
		async with self._session.post(
				url, json=body, headers=_CONTENT_TYPE_HEADER) as r:
			return await self._handle_response(r, hat=hat, send=False)

	# noinspection PyTypeChecker
	@codetiming.Timer(text=_WRITE_SCORES_MSG, logger=stdout)
	async def post_scores(
			self,
			token: str,
			*,
			scores: Iterable[Tuple[str, _model.RiskScore]],
			namespace: str) -> NoReturn:
		"""Sends computed exposure scores to all PDAs."""
		namespace = '/'.join((self.client_namespace, namespace))
		timestamp = time.time() * 1e3

		async def post(hat: str, score: _model.RiskScore):
			value = round(score.value, 2)
			body = {
				'token': token,
				'contractId': self.contract_id,
				'hatName': hat,
				'body': {'score': value, 'timestamp': timestamp}}
			url = self._format_url(self.write_url, hat, namespace)
			async with self._session.post(
					url, json=body, headers=_CONTENT_TYPE_HEADER) as r:
				await self._handle_response(r, hat=hat)

		await asyncio.gather(*[post(h, s) for h, s in scores])

	@staticmethod
	async def _handle_response(
			response: aiohttp.ClientResponse,
			hat: Optional[str] = None,
			send: Optional[bool] = True) -> _Response:
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

	@staticmethod
	def _format_url(base_url: str, hat: str, namespace: str):
		with_hat = ''.join((_HTTPS, f'{hat}.', base_url.split(_HTTPS)[-1]))
		return '/'.join((with_hat, namespace))


def _to_timestamp(ms_timestamp: float):
	return datetime.datetime.utcfromtimestamp(ms_timestamp / 1e3)

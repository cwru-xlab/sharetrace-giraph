import asyncio
import datetime
import itertools
import time
from typing import Any, Iterable, Mapping, NoReturn, Optional, Tuple, Union

import aiohttp
import attr
import codetiming
import jsonpickle
import numpy as np
from attr import validators

import backend
import model

SUCCESS_CODES = {200, 201}
CONTENT_TYPE = {'Content-Type': 'application/json'}
HTTPS = 'https://'
BASE_READ_BODY = {'orderBy': 'timestamp', 'ordering': 'descending', 'skip': 0}
TWO_WEEKS_AGO = datetime.datetime.utcnow() - datetime.timedelta(days=14)
Response = Optional[Union[Mapping[str, Any], Iterable[Mapping[str, Any]]]]
# Timing messages
READ_HATS_MSG = 'Reading tokens and hats: {:0.6f} s'
READ_SCORES_MSG = 'Reading scores: {:0.6f} s'
READ_LOCATIONS_MSG = 'Reading location histories: {:0.6f} s'
WRITE_SCORES_MSG = 'Writing scores: {:0.6f} s'
stdout = backend.STDOUT
stderr = backend.STDERR


# noinspection PyUnresolvedReferences
@attr.s(slots=True)
class PdaContext:
	"""Contracted PDA context manager for the ShareTrace project.

	Communicates with user PDAs to retrieve data to send back computed
	risk scores. The client namespace, contract ID, and long-lived token are
	required in order to use the context. All other attributes can be set
	post-instantiation.

	Attributes:
		client_namespace: Endpoint containing the contracted namespaces.
		contract_id: A unique value that identifies the contract.
		keyring_url: Endpoint from which to retrieve a short-lived token.
		read_url: Endpoint from which to read contracted PDA data.
		write_url: Endpoint from which to write contracted PDA data.
		long_lived_token: Required to retrieve a short-lived token and send
			requests.
	"""

	client_namespace = attr.ib(type=str, validator=validators.instance_of(str))
	contract_id = attr.ib(type=str, validator=validators.instance_of(str))
	long_lived_token = attr.ib(type=str, validator=validators.instance_of(str))
	keyring_url = attr.ib(
		type=str, validator=validators.instance_of(str), default=None)
	read_url = attr.ib(
		type=str, validator=validators.instance_of(str), default=None)
	write_url = attr.ib(
		type=str, validator=validators.instance_of(str), default=None)
	_session = attr.ib(type=aiohttp.ClientSession, init=False, repr=False)

	async def __aenter__(self):
		self._session = aiohttp.ClientSession()
		return self

	async def __aexit__(self, exc_type, exc_val, exc_tb):
		await self._session.close()

	@codetiming.Timer(text=READ_HATS_MSG, logger=stdout)
	async def get_hats_and_token(self) -> Tuple[Iterable[str], str]:
		"""Retrieves a short-lived token and contracted-associated HATs."""
		headers = {'Authorization': f'Bearer {self.long_lived_token}'}
		async with self._session.get(self.keyring_url, headers=headers) as r:
			if response := await self._handle_response(r):
				token, hats = response['token'], response['associatedHats']
				stdout(f'Number of hats retrieved: {len(hats)}')
			else:
				raise IOError(
					'No HATs could be retrieved. Check that HATs are '
					'associated with the contract ID.')
			return np.array(hats), token

	@codetiming.Timer(text=READ_SCORES_MSG, logger=stdout)
	async def get_scores(
			self,
			hats: Iterable[str],
			*,
			token: str,
			namespace: str,
			take: int = None,
			since: datetime.datetime = TWO_WEEKS_AGO
	) -> Iterable[Tuple[str, Iterable[model.RiskScore]]]:
		"""Retrieves the survey risk scores from the PDAs.

		Each response record to is mapped to a collection of RiskScore objects.
		RiskScore objects are grouped by HAT.
		"""
		namespace = '/'.join((self.client_namespace, namespace))
		h1, h2 = itertools.tee(hats)
		data = await asyncio.gather(
			*(self._get_data(h, token, namespace, take) for h in h1))
		hats_and_data = ((h, d) for h, d in zip(h2, data) if d)
		return (self._to_scores(h, d, since) for h, d in hats_and_data)

	@staticmethod
	def _to_scores(
			hat: str,
			data: Iterable[Mapping[str, Any]],
			since: datetime.datetime = TWO_WEEKS_AGO
	) -> Tuple[str, Iterable[model.RiskScore]]:
		values = (s['data'] for s in data)
		values = ((s['score'], _to_timestamp(s['timestamp'])) for s in values)
		scores = (
			model.RiskScore(name=hat, value=v, timestamp=t)
			for v, t in values if t >= since)
		return hat, scores

	@codetiming.Timer(text=READ_LOCATIONS_MSG, logger=stdout)
	async def get_locations(
			self,
			hats: Iterable[str],
			*,
			token: str,
			namespace: str,
			take: int = None,
			since: datetime.datetime = TWO_WEEKS_AGO,
			obfuscation: int = 3) -> Iterable[model.LocationHistory]:
		"""Retrieves the location data from the PDAs.

		Maps each response record to a LocationHistory object.
		"""
		namespace = '/'.join((self.client_namespace, namespace))
		h1, h2 = itertools.tee(hats)
		data = await asyncio.gather(
			*(self._get_data(h, token, namespace, take) for h in h1))
		hats_and_data = ((h, d) for h, d in zip(h2, data) if d)
		return (
			self._to_locations(h, d, since, obfuscation)
			for h, d in hats_and_data)

	@staticmethod
	def _to_locations(
			hat: str,
			data: Iterable[Mapping[str, Any]],
			since: datetime.datetime,
			obfuscation: int) -> model.LocationHistory:
		locations = (loc['data'] for loc in data)
		locations = (
			(_to_timestamp(loc['timestamp']), loc['hash'][:-obfuscation])
			for loc in locations)
		locations = (
			model.TemporalLocation(timestamp=t, location=h)
			for t, h in locations if t >= since)
		return model.LocationHistory(name=hat, history=locations)

	async def _get_data(
			self,
			hat: str,
			token: str,
			namespace: str,
			take: int) -> Response:
		body = BASE_READ_BODY.copy()
		body.update({
			'hatName': hat,
			'token': token,
			'contractId': self.contract_id})
		if take is not None:
			body['take'] = take
		url = self._format_url(self.read_url, hat, namespace)
		async with self._session.post(
				url, json=body, headers=CONTENT_TYPE) as r:
			return await self._handle_response(r, hat=hat, send=False)

	# noinspection PyTypeChecker
	@codetiming.Timer(text=WRITE_SCORES_MSG, logger=stdout)
	async def post_scores(
			self,
			scores: Iterable[model.RiskScore],
			*,
			token: str,
			namespace: str) -> NoReturn:
		"""Sends computed exposure scores to all PDAs."""
		namespace = '/'.join((self.client_namespace, namespace))
		timestamp = time.time() * 1e3

		async def post(score: model.RiskScore):
			value = round(score.value, 2)
			hat = score.name
			body = {
				'token': token,
				'contractId': self.contract_id,
				'hatName': hat,
				'body': {'score': value, 'timestamp': timestamp}}
			url = self._format_url(self.write_url, hat, namespace)
			async with self._session.post(
					url, json=body, headers=CONTENT_TYPE) as r:
				return await self._handle_response(r, hat=hat, send=True)

		response = await asyncio.gather(*(post(s) for s in scores))
		total = len(response)
		num_failed = sum(map(lambda r: r is None, response))
		stdout(f'Number of successful posts: {total - num_failed}')
		stderr(f'Number of failed posts: {num_failed}')

	@staticmethod
	async def _handle_response(
			response: aiohttp.ClientResponse,
			hat: Optional[str] = None,
			send: Optional[bool] = None) -> Response:
		def check_hat():
			if send is not None and hat is None:
				raise ValueError(
					'Must provide HAT name to handle non-keyring response')

		content = await response.read()
		if (status := response.status) not in SUCCESS_CODES:
			content = None
			if send:
				check_hat()
				stderr(f'{status}: failed to send data to {hat}')
			elif send is False:
				check_hat()
				stderr(f'{status}: failed to get data from {hat}')
			else:
				raise IOError(f'{status}: failed to authorize keyring')
		else:
			content = jsonpickle.loads(content)
		return content

	@staticmethod
	def _format_url(base_url: str, hat: str, namespace: str):
		with_hat = ''.join((HTTPS, f'{hat}.', base_url.split(HTTPS)[-1]))
		return '/'.join((with_hat, namespace))


def _to_timestamp(ms_timestamp: float):
	return datetime.datetime.utcfromtimestamp(ms_timestamp / 1e3)

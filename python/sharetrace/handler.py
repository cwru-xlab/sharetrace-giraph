import asyncio
import datetime
import logging
import os
from base64 import b64decode

import boto3
import codetiming
import jsonpickle
from aws_xray_sdk.core import patch_all

import backend
import pda
import propagation
import search

logger = logging.getLogger()
logger.setLevel(logging.INFO)
patch_all()
backend.set_stdout(logger.info)
backend.set_stderr(logger.error)
stdout = backend.STDOUT
stderr = backend.STDERR

backend.set_local_mode(True)
backend.set_time(datetime.datetime.utcnow())

lambda_client = boto3.client('lambda')
lambda_client.get_account_settings()
kms_client = boto3.client('kms')


def _decrypt(value):
	context = {'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
	decrypted = kms_client.decrypt(
		CiphertextBlob=b64decode(value), EncryptionContext=context)
	return decrypted('Plaintext').decode('utf-8')


# PDA environment variables
CONTRACT_ID = _decrypt(os.environ['CONTRACT_ID'])
LONG_LIVED_TOKEN = _decrypt(os.environ['LONG_LIVED_TOKEN'])
CLIENT_NAMESPACE = _decrypt(os.environ['CLIENT_NAMESPACE'])
READ_SCORE_NAMESPACE = _decrypt(os.environ['READ_SCORE_NAMESPACE'])
WRITE_SCORE_NAMESPACE = _decrypt(os.environ['WRITE_SCORE_NAMESPACE'])
LOCATION_NAMESPACE = _decrypt(os.environ['READ_LOCATION_NAMESPACE'])
TAKE_SCORES = int(os.environ['TAKE_SCORES'])
TAKE_LOCATIONS = int(os.environ['TAKE_LOCATIONS'])
KEYRING_URL = os.environ['KEYRING_URL']
READ_URL = os.environ['READ_URL']
WRITE_URL = os.environ['WRITE_URL']
KWARGS = {
	'client_namespace': CLIENT_NAMESPACE,
	'contract_id': CONTRACT_ID,
	'keyring_url': KEYRING_URL,
	'read_url': READ_URL,
	'write_url': WRITE_URL,
	'long_lived_token': LONG_LIVED_TOKEN}
# Contact search environment variables
MIN_DURATION = datetime.timedelta(seconds=float(os.environ['MIN_DURATION']))
# Propagation environment variables
TRANSMISSION_RATE = float(os.environ['TRANSMISSION_RATE'])
ITERATIONS = int(os.environ['ITERATIONS'])
TOLERANCE = float(os.environ['TOLERANCE'])
SCORE_TIMESTAMP_BUFFER = datetime.timedelta(
	seconds=float(os.environ['SCORE_TIMESTAMP_BUFFER']))


def handle(event, context):
	environment = jsonpickle.encode(dict(**os.environ))
	stdout(f'## ENVIRONMENT VARIABLES\n{environment}')
	stdout(f'## EVENT\n{jsonpickle.encode(event)}')
	stdout(f'## CONTEXT\n{jsonpickle.encode(context)}')
	stdout('------------------START TASK------------------')
	asyncio.get_event_loop().run_until_complete(_ahandle(event, context))
	stdout('-------------------END TASK-------------------')


@codetiming.Timer(text='Total task duration: {:0.6f} s', logger=stdout)
async def _ahandle(event, context):
	def compute(locs, users):
		contact_search = search.ContactSearch(min_duration=MIN_DURATION)
		factors = contact_search(locs)
		prop = propagation.BeliefPropagation(
			transmission_rate=TRANSMISSION_RATE,
			iterations=ITERATIONS,
			tolerance=TOLERANCE,
			timestamp_buffer=SCORE_TIMESTAMP_BUFFER)
		return prop(factors=factors, variables=users)

	async with pda.PdaContext(**KWARGS) as p:
		token, hats = await p.get_token_and_hats()
		variables, locations = await asyncio.gather(
			p.get_scores(
				token,
				hats=hats,
				namespace=READ_SCORE_NAMESPACE,
				take=TAKE_SCORES),
			p.get_locations(
				token,
				hats=hats,
				namespace=LOCATION_NAMESPACE,
				take=TAKE_LOCATIONS))
	if backend.LOCAL_MODE:
		updated_scores = compute(locations, variables)
	else:
		with backend.ray_context():
			updated_scores = compute(locations, variables)
	async with pda.PdaContext(**KWARGS) as p:
		await p.post_scores(
			token, scores=updated_scores, namespace=WRITE_SCORE_NAMESPACE)

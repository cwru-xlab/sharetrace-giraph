import asyncio
import base64
import datetime
import os
from typing import NoReturn, Union

import boto3
import codetiming
import jsonpickle

import backend
import pda
import propagation
import search

stdout = backend.STDOUT
stderr = backend.STDERR

lambda_client = boto3.client('lambda')
kms_client = boto3.client('kms')


def _since(days: float) -> datetime.datetime:
	if days < 0:
		since = datetime.datetime.min
	else:
		since = datetime.datetime.utcnow() - datetime.timedelta(days=days)
	return since


def _decrypt(value: Union[str, bytes]) -> str:
	"""Decrypts an encrypted environment variable"""
	context = {'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
	decrypted = kms_client.decrypt(
		CiphertextBlob=base64.b64decode(value, validate=True),
		EncryptionContext=context)
	return decrypted['Plaintext'].decode('utf-8')


# PDA environment variables
CONTRACT_ID = _decrypt(os.environ['CONTRACT_ID'])
LONG_LIVED_TOKEN = _decrypt(os.environ['LONG_LIVED_TOKEN'])
CLIENT_NAMESPACE = _decrypt(os.environ['CLIENT_NAMESPACE'])
READ_SCORE_NAMESPACE = _decrypt(os.environ['READ_SCORE_NAMESPACE'])
WRITE_SCORE_NAMESPACE = _decrypt(os.environ['WRITE_SCORE_NAMESPACE'])
LOCATION_NAMESPACE = _decrypt(os.environ['READ_LOCATION_NAMESPACE'])
TAKE_SCORES = int(os.environ['TAKE_SCORES'])
TAKE_LOCATIONS = int(os.environ['TAKE_LOCATIONS'])
HASH_OBFUSCATION = int(os.environ['HASH_OBFUSCATION'])
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
TIMESTAMP_BUFFER = datetime.timedelta(
	days=float(os.environ['TIMESTAMP_BUFFER']))
SEND_THRESHOLD = float(os.environ['SEND_THRESHOLD'])
SEND_CONDITION = os.environ['SEND_CONDITION']
TIME_CONSTANT = float(os.environ['TIME_CONSTANT'])
LOOKBACK_DAYS = _since(float(os.environ['LOOKBACK_DAYS']))


def handle(event, context):
	"""Communicates with user PDAs to compute exposure scores.

	Args:
		event: AWS scheduled CloudWatch Event. Format:
			{
				"account": "123456789012",
				"region": "us-east-2",
				"detail": {},
				"detail-type": "Scheduled Event",
				"source": "aws.events",
				"time": "2019-03-01T01:23:45Z",
				"id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
				"resources": [
					"arn:aws:events:us-east-1:123456789012:rule/my-schedule"
				]
			}
		context: Contains various AWS Lambda runtime variables.

	Returns: Mapping that contains the status code of the execution.

	References:
		https://docs.aws.amazon.com/lambda/latest/dg/services-cloudwatchevents
		.html
		https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
	"""
	environment = jsonpickle.encode(dict(**os.environ))
	stdout(f'## ENVIRONMENT VARIABLES\n{environment}')
	stdout(f'## EVENT\n{jsonpickle.encode(event)}')
	stdout(f'## CONTEXT\n{jsonpickle.encode(context)}')
	stdout('------------------START TASK------------------')
	asyncio.run(_handle())
	stdout('-------------------END TASK-------------------')
	return {'status_code': 200}


# noinspection PyTypeChecker
@codetiming.Timer(text='Total task duration: {:0.6f} s', logger=stdout)
async def _handle() -> NoReturn:
	async with pda.PdaContext(**KWARGS) as p:
		hats, token = await p.get_hats_and_token()
		variables, locations = await asyncio.gather(
			p.get_scores(
				hats=hats,
				token=token,
				namespace=READ_SCORE_NAMESPACE,
				take=TAKE_SCORES,
				since=LOOKBACK_DAYS),
			p.get_locations(
				hats=hats,
				token=token,
				namespace=LOCATION_NAMESPACE,
				take=TAKE_LOCATIONS,
				obfuscation=HASH_OBFUSCATION,
				since=LOOKBACK_DAYS))
		contact_search = search.ContactSearch(min_duration=MIN_DURATION)
		factors = contact_search(locations)
		belief_propagation = propagation.LocalBeliefPropagation(
			transmission_rate=TRANSMISSION_RATE,
			iterations=ITERATIONS,
			tolerance=TOLERANCE,
			time_buffer=TIMESTAMP_BUFFER,
			send_threshold=SEND_THRESHOLD,
			send_condition=SEND_CONDITION,
			time_constant=TIME_CONSTANT)
		updated_scores = belief_propagation(factors, variables)
		await p.post_scores(
			scores=updated_scores,
			token=token,
			namespace=WRITE_SCORE_NAMESPACE)

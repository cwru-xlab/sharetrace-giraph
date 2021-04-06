import abc
import asyncio
import datetime
import functools
import itertools
import numbers
import random
from typing import (
	Any, Callable, Collection, Hashable, Iterable, NoReturn,
	Optional, Sequence, Tuple, Union)

import attr
import codetiming
import numpy as np
import ray
from attr import validators

import backend
import graphs
import model
import stores

"""
Local-remote trade-off:
	System information:
		System model: Dell XPS 15 9560
		Physical memory: 16GB RAM
		Processor: Intel i7-7700HQ CPU @ 2.8GHz
		Cores (logical/physical): 8/4
	
	Simulation setup:
		Users: 500, 1000
		Scores per user: 14
		Days: 14
		Unique locations: 10
		Graph implementation: Numpy
		Iterations (local/remote): 4/550
		Tolerance: 1e-6
		Transmission factor: 0.8
		Time constant: 1
		Time buffer: 2 days
		Seed: 12345

	Timing:
		500 users (local/remote): 346/374 (1.08x faster)
		1000 users (local/remote): 1330/788 (0.59x faster)

		Linear interpolation: ~600 users leads to 1:1 performance
"""

# Globals
_TWO_DAYS = np.timedelta64(2, 'D')
_DEFAULT_MESSAGE = model.RiskScore(
	name='DEFAULT_ID', timestamp=datetime.datetime.utcnow(), value=0)
_SEND_BY_LOCAL = 'local'
_SEND_BY_MESSAGE = 'message'
_SEND_CONDITION_OPTIONS = {_SEND_BY_LOCAL, _SEND_BY_MESSAGE}
stdout = backend.STDOUT
stderr = backend.STDERR
# Type aliases
TimestampBuffer = Union[datetime.timedelta, np.timedelta64]
RiskScores = Iterable[model.RiskScore]
AllRiskScores = Collection[Tuple[Hashable, RiskScores]]
Contacts = Iterable[model.Contact]
StopCondition = Callable[..., bool]
SendCondition = Callable[..., bool]


# noinspection PyUnresolvedReferences
@attr.s(slots=True)
class BeliefPropagation(abc.ABC):
	"""Runs the belief propagation algorithm to compute exposure scores.

	A factor vertex represents a pair of people, with vertex data containing
	all occurrences (time-duration pairs) in the recent past that the two
	individuals came into sufficiently long contact. A variable vertex
	represents an individual, with vertex data containing the local risk
	scores that are derived from user-reported symptoms, the maximum of these
	scores, and all risk scores sent from neighboring factor vertices.

	Following the core message-passing principle of belief propagation, the
	algorithm performs iterative computation between the factor and variable
	vertex sets. The algorithm begins with all variable vertices selecting
	their maximum local score and sending it to all of their neighboring
	factor vertices. Then, all factor vertices filter the risk scores,
	based on when the individuals came into contact. That is, only risk
	scores recorded prior to at least one occurrence that defines the contact
	are retained for subsequent calculation. A weighted transformation that
	accounts for the amount of time passed from when the risk score was
	recorded and the time of running the algorithm is applied to all risk
	scores from a variable vertex. The maximum of these is sent to the other
	variable vertex connected to the factor vertex.

	This completes one iteration of the algorithm and is repeated until
	either a certain number of iterations has passed or the summed difference
	in maximum variable risk scores from the previous iteration drops below a
	set tolerance, whichever condition is satisfied first.

	Attributes:
		transmission_rate: The scaling applied to each weighted risk score at
			the factor vertex. A value of 1 implies that only the effect of
			the exponential time decay weighting scheme affects which risk
			score is sent to the opposite variable vertex.
		tolerance: The minimum value that must be exceeded to continue onto
			the next iteration. The summed difference of all variable
			vertices' max risk scores between the previous iteration and
			current iteration must exceed the tolerance value to continue
			onto the next iteration. The difference is measured, per message
			sent from factors, rather than per variable. Thus, a max risk
			score could be updated multiple times, and all are included in
			the sum.
		iterations: The maximum number of loops the algorithm will run. If
			the tolerance value is not exceeded, then it may not be the case
			that all iterations are completed. For local computing, this
			defines the number of times the variable vertices compute. For
			remote computing, this defines the number of messages a variable
			Ray actor should process before re-calculating the tolerance. If
			unsure about the number of variables and initial messages each
			variable has to send, set this to a high number. Set this to be
			at least twice (num_variables) x (num_messages_per_variable) to
			ensure that the factors have enough time to start sending
			messages in response.
		time_buffer: The amount of time (in seconds) that must have passed
			between an occurrence and risk score for it to be retained
			by the factor vertex during computation.
		time_constant: The exponential time constant used when weighting the
			value of each valid message received by a factor vertex.
		queue_size: Size of the queue used to store incoming messages to
			either a factor or variable partition actor. Only active when
			running RemoteBeliefPropagation.
		send_threshold: The lower bound (exclusive) on the value of the
			messages sent by variable vertices.
		send_condition: Determines when a variable vertex sends a message.
			Options:
				- 'message': Sends a message of its value is higher than
					send_threshold.
				- 'local': Sends a message of its value is at least
					send_threshold % of the current local value of the
					variable vertex.
		impl: Implementation to use for the factor graph.
		seed: Random seed to allow for reproducibility. If left unset,
			the current seed is used.
	"""
	transmission_rate = attr.ib(
		type=numbers.Real,
		default=0.8,
		validator=validators.instance_of(numbers.Real),
		converter=float,
		kw_only=True)
	tolerance = attr.ib(
		type=numbers.Real,
		default=1e-6,
		validator=validators.instance_of(numbers.Real),
		converter=float,
		kw_only=True)
	iterations = attr.ib(
		type=numbers.Real,
		default=4,
		validator=validators.instance_of(int),
		kw_only=True)
	time_buffer = attr.ib(
		type=TimestampBuffer,
		default=_TWO_DAYS,
		validator=validators.instance_of((datetime.timedelta, np.timedelta64)),
		converter=lambda x: np.timedelta64(x, 's'),
		kw_only=True)
	time_constant = attr.ib(
		type=numbers.Real,
		default=1,
		validator=validators.instance_of(numbers.Real),
		converter=float,
		kw_only=True)
	queue_size = attr.ib(
		type=int,
		default=0,
		validator=validators.instance_of(int),
		kw_only=True)
	send_threshold = attr.ib(
		type=numbers.Real,
		default=0.75,
		validator=validators.instance_of(numbers.Real),
		converter=float,
		kw_only=True)
	send_condition = attr.ib(type=str, default=_SEND_BY_LOCAL, kw_only=True)
	default_msg = attr.ib(
		type=model.RiskScore,
		default=_DEFAULT_MESSAGE,
		validator=validators.instance_of(model.RiskScore),
		kw_only=True)
	impl = attr.ib(
		type=str,
		default=graphs.DEFAULT,
		validator=validators.in_(graphs.OPTIONS),
		kw_only=True)
	seed = attr.ib(type=Optional[Any], default=None, kw_only=True)

	def __attrs_post_init__(self):
		super(BeliefPropagation, self).__init__()
		if self.seed is not None:
			random.seed(self.seed)
			np.random.seed(self.seed)

	@transmission_rate.validator
	def _check_transmission_rate(self, _, value):
		if value < 0 or float(value) > 1:
			raise ValueError(
				"'transmission_rate' must be between 0 and 1, inclusive")

	@tolerance.validator
	def _check_tolerance(self, _, value):
		if value <= 0:
			raise ValueError(
				f"'tolerance' must be greater than 0; got {value}")

	@iterations.validator
	def _check_iterations(self, _, value):
		if value < 1:
			raise ValueError(f"'iterations' must be at least 1; got {value}")

	@send_condition.validator
	def _check_send_condition(self, _, value):
		if value not in _SEND_CONDITION_OPTIONS:
			raise ValueError(
				f"'send_condition' must be one of the following:"
				f" {_SEND_CONDITION_OPTIONS}; got {value}")

	@abc.abstractmethod
	def call(self, *args, **kwargs):
		"""Runs belief propagation until the stopping condition is met."""
		pass

	@abc.abstractmethod
	def create_graph(self, *args, **kwargs):
		"""Creates the factor graph."""
		pass

	@abc.abstractmethod
	def should_send(self, *args, **kwargs) -> bool:
		"""Returns True if a message should be sent by a variable vertex."""
		pass

	@abc.abstractmethod
	def should_stop(self, *args, **kwargs) -> bool:
		"""Returns True if the algorithm should stop running."""
		pass


@attr.s(slots=True)
class LocalBeliefPropagation(BeliefPropagation):
	"""A single-process implementation of BeliefPropagation."""
	_graph = attr.ib(type=graphs.FactorGraph, init=False, repr=False)
	_variables = attr.ib(type=stores.VertexStore, init=False, repr=False)
	_factors = attr.ib(type=stores.VertexStore, init=False, repr=False)

	def __attrs_post_init__(self):
		super(LocalBeliefPropagation, self).__attrs_post_init__()

	def __call__(
			self, factors: Contacts, variables: AllRiskScores) -> RiskScores:
		stdout('-----------START BELIEF PROPAGATION-----------')
		result = self.call(factors, variables)
		stdout('------------END BELIEF PROPAGATION------------')
		return result

	@codetiming.Timer(text='Total duration: {:0.6f} s', logger=stdout)
	def call(
			self,
			factors: Contacts,
			variables: AllRiskScores,
			**kwargs) -> RiskScores:
		self.create_graph(factors, variables)
		i, epoch, stop = 1, None, False
		while not stop:
			stdout(f'-----------Iteration {i}-----------')
			epoch = self._send_to_factors()
			self._send_to_variables()
			stop = self.should_stop(i, epoch)
			i += 1
			stdout(f'---------------------------------')
		return (self._variables.get(v, 'local') for v in self._variables)

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Creating graph: {:0.6f} s', logger=stdout)
	def create_graph(
			self,
			factors: Contacts,
			variables: AllRiskScores,
			**kwargs):
		builder = graphs.FactorGraphBuilder(
			impl=self.impl,
			share_graph=False,
			graph_as_actor=False,
			use_vertex_store=True,
			num_stores=(1, 1),
			store_as_actor=False,
			detached=False)
		# Must add variables before adding edges
		self._add_variables(builder, variables)
		self._add_factors_and_edges(builder, factors)
		self._graph, _, _, (factors, variables) = builder.build()
		self._factors = factors[0]
		self._variables = variables[0]

	# noinspection PyTypeChecker
	def _add_variables(
			self,
			builder: graphs.FactorGraphBuilder,
			variables: AllRiskScores) -> NoReturn:
		vertices = {}
		for k, v in ((str(k), v) for k, v in variables):
			v1, v2 = itertools.tee(v)
			vertices[k] = {
				'local': max(v1, default=self.default_msg),
				'inbox': {k: np.array(list(v2))}}
		if vertices:
			builder.add_variables(vertices)
		else:
			raise ValueError('There must be at least 1 variable')

	# noinspection PyTypeChecker
	@staticmethod
	def _add_factors_and_edges(
			builder: graphs.FactorGraphBuilder,
			factors: Contacts) -> NoReturn:
		def make_key(factor: model.Contact):
			parts = tuple(str(u) for u in sorted(factor.users))
			key = '_'.join(parts)
			return key, parts

		vertices = {}
		edges = []
		for f in factors:
			k, (v1, v2) = make_key(f)
			edges.extend(((k, v1), (k, v2)))
			occurrences = (o.as_array() for o in f.occurrences)
			vertices[k] = {
				'occurrences': np.array(list(occurrences)).flatten(),
				'inbox': {}}
		if vertices:
			builder.add_factors(vertices)
			builder.add_edges(edges)
		else:
			raise ValueError('There must be at least 1 factor')

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Sending to factors: {:0.6f} s', logger=stdout)
	def _send_to_factors(self) -> np.ndarray:
		def update_local(variable, local, incoming):
			# First iteration only: messages only from self
			if variable in incoming:
				values = itertools.chain([local], incoming[variable])
			else:
				values = itertools.chain([local], incoming.values())
			if (updated := max(values)) > local:
				difference = updated.value - local.value
				self._variables.put({variable: {'local': updated}})
			else:
				difference = 0
			return difference

		def send(sender, local, incoming):
			should_send = functools.partial(self.should_send, local)
			for f in self._graph.get_neighbors(sender):
				# First iteration only: messages only from self
				if sender in incoming:
					others = (m for m in incoming[sender] if should_send(m))
				else:
					others = (
						msg for o, msg in incoming.items()
						if o != f and should_send(msg))
				self._factors.put({f: {'inbox': {sender: others}}}, merge=True)

		epoch = []
		for v in self._variables:
			curr_local = self._variables.get(v, 'local')
			inbox = self._variables.get(v, 'inbox')
			diff = update_local(v, curr_local, inbox)
			epoch.append(diff)
			send(v, curr_local, inbox)
			self._variables.put({v: {'inbox': {}}})
		return np.array(epoch)

	def should_send(self, local, incoming, **kwargs) -> bool:
		return _should_send(
			local, incoming, self.send_condition, self.send_threshold)

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Sending to variables: {:0.6f} s', logger=stdout)
	def _send_to_variables(self) -> NoReturn:
		for f in self._factors:
			neighbors = tuple(self._graph.get_neighbors(f))
			inbox = self._factors.get(f, 'inbox')
			for i, n in enumerate(neighbors):
				# Assumes factor vertex has a degree of 2
				receiver = neighbors[not i]
				msg = self._compute_message(f, inbox[n])
				msg = {'inbox': {f: msg}}
				self._variables.put({receiver: msg}, merge=True)
			self._factors.put({f: {'inbox': {}}})

	def _compute_message(
			self,
			factor: graphs.Vertex,
			msg: Iterable[model.RiskScore]) -> model.RiskScore:
		def sec_to_day(s: Union[np.ndarray, np.float64]):
			return np.array(s / 86_400, dtype=np.float64)

		occurrences = self._factors.get(factor, 'occurrences')
		most_recent = np.max(occurrences['timestamp'])
		msg = np.array([m.as_array() for m in msg]).flatten()
		if msg.size:  # Are there are any messages?
			old = np.where(msg['timestamp'] <= most_recent + self.time_buffer)
			msg = msg[old]
		if msg.size:  # Are there any valid messages?
			# Formats time delta as partial days
			diff = sec_to_day(msg['timestamp'] - most_recent)
			# Weight can only decrease original message value
			np.clip(diff, -np.inf, 0, out=diff)
			msg['value'] *= np.exp(diff / self.time_constant)
			msg['value'] *= self.transmission_rate
			msg.sort(order=['value', 'timestamp', 'name'])
			msg = model.RiskScore.from_array(msg[-1])
		else:
			msg = self.default_msg
		return msg

	def should_stop(
			self, iteration: int, epoch: np.ndarray = None, **kwargs) -> bool:
		if iteration == 1:
			stop = False
			stdout(f'Epoch tolerance: n/a')
		else:
			diff = sum(epoch)
			stop = diff < self.tolerance or iteration >= self.iterations
			stdout(f'Epoch tolerance: {np.round(diff, 6)}')
		return stop


@ray.remote
class _ShareTraceVariablePart:
	__slots__ = [
		'graph',
		'factors',
		'vertex_store',
		'queue',
		'local_msgs',
		'block_queue',
		'send_threshold',
		'should_send',
		'iterations',
		'epoch',
		'should_stop']

	def __init__(
			self,
			vertex_store: stores.VertexStore,
			queue_size: int,
			local_msgs: Sequence[model.Message],
			send_threshold: float,
			should_send: SendCondition,
			iterations: int,
			should_stop: StopCondition):
		self.graph = None
		self.factors = None
		self.vertex_store = vertex_store
		self.block_queue = queue_size > 0
		self.queue = stores.queue_factory(
			max_size=queue_size, asynchronous=True, local_mode=True)
		self.local_msgs = local_msgs
		self.send_threshold = send_threshold
		self.should_send = should_send
		self.iterations = iterations
		self.should_stop = should_stop

	async def send_to_factors(
			self,
			graph: graphs.FactorGraph,
			factors: Sequence['_ShareTraceFactorPart']) -> RiskScores:
		self.graph = graph
		self.factors = factors
		await self._send_local()
		stop, i = False, 1
		while not stop:
			text = ': '.join((f'Iteration {i}', '{:0.6f} s'))
			with codetiming.Timer(text=text, logger=stdout):
				epoch = await self._send_incoming()
				stop = self.should_stop(epoch)
				i += 1
		local = functools.partial(lambda v: self.vertex_store.get(v, 'local'))
		return np.array([local(v) for v in self.vertex_store])

	@codetiming.Timer(text='Sending local messages: {:0.6f} s', logger=stdout)
	async def _send_local(self):
		await asyncio.gather(*(self._send(m) for m in self.local_msgs))
		self.local_msgs = None

	async def _send_incoming(self) -> Iterable[float]:
		epoch = []
		# Cannot use asyncio.gather: must keep message passing active
		for _ in range(self.iterations):
			msg = await self.queue.get(block=True)
			await self._send(msg)
			diff = self._update_local(msg)
			epoch.append(diff)
		return epoch

	def _update_local(self, msg: model.Message) -> float:
		receiver = msg.receiver
		local = self.vertex_store.get(receiver, 'local')
		if (updated := max(msg.content, local)) > local:
			diff = updated.value - local.value
			self.vertex_store.put({receiver: {'local': updated}})
		else:
			diff = 0
		return diff

	async def _send(self, msg: model.Message) -> NoReturn:
		# Receiver becomes the sender and vice versa
		sender, receiver, msg = msg.receiver, msg.sender, msg.content
		# Avoid self-bias: do not send message back to sending factor
		others = (n for n in self.graph.get_neighbors(sender) if n != receiver)
		# TODO(rdt17) Consider reverting using for loop
		await asyncio.gather(*(self._enqueue(sender, f, msg) for f in others))

	async def _enqueue(self, sender, receiver, msg: model.RiskScore):
		local = self.vertex_store.get(sender, 'local')
		if self.should_send(local, msg):
			msg = model.Message(sender=sender, receiver=receiver, content=msg)
			f = self.graph.get_vertex_attr(receiver, 'address')
			await self.factors[f].enqueue.remote(msg)

	async def enqueue(self, *msgs: model.Message) -> NoReturn:
		# TODO(rdt17) Consider reverting back to asyncio.gather()
		for m in msgs:
			await self.queue.put(m, block=True)

	# noinspection PyTypeChecker
	@staticmethod
	def kill() -> NoReturn:
		ray.actor.exit_actor()


@ray.remote
class _ShareTraceFactorPart:
	__slots__ = [
		'vertex_store',
		'queue',
		'block_queue',
		'transmission_rate',
		'time_buffer',
		'time_constant',
		'send_threshold',
		'default_msg']

	def __init__(
			self,
			vertex_store: stores.VertexStore,
			queue_size: int,
			transmission_rate: float,
			time_buffer: TimestampBuffer,
			time_constant: float,
			default_msg: model.RiskScore):
		self.vertex_store = vertex_store
		self.block_queue = queue_size > 0
		self.queue = stores.queue_factory(
			max_size=queue_size, asynchronous=True, local_mode=True)
		self.transmission_rate = transmission_rate
		self.time_buffer = time_buffer
		self.time_constant = time_constant
		self.default_msg = default_msg

	async def send_to_variables(
			self,
			graph: graphs.FactorGraph,
			variables: Sequence['_ShareTraceVariablePart']) -> NoReturn:
		while True:
			msg = await self.queue.get(block=True)
			# Receiver becomes the sender
			sender = msg.receiver
			neighbors = tuple(graph.get_neighbors(sender))
			# Assumes factor vertex has a degree of 2
			receiver = neighbors[not neighbors.index(msg.sender)]
			msg = self._compute_message(sender, msg.content)
			msg = model.Message(sender=sender, receiver=receiver, content=msg)
			v = graph.get_vertex_attr(receiver, key='address')
			await variables[v].enqueue.remote(msg)

	# TODO(rdt17) Refactor to be callable passed into class
	def _compute_message(
			self,
			factor: graphs.Vertex,
			msg: model.RiskScore) -> model.RiskScore:
		def sec_to_day(s: Union[np.ndarray, np.float64]):
			return np.divide(np.float64(s), 86400)

		occurrences = self.vertex_store.get(factor, 'occurrences')
		msg = msg.as_array()
		most_recent = np.max(occurrences['timestamp'])
		if msg['timestamp'] <= most_recent + self.time_buffer:
			# Formats time delta as partial days
			diff = sec_to_day(msg['timestamp'] - most_recent)
			# Weight can only decrease original message value
			np.clip(diff, -np.inf, 0, out=diff)
			msg['value'] *= np.exp(diff / self.time_constant)
			msg['value'] *= self.transmission_rate
			msg = model.RiskScore.from_array(msg)
		else:
			msg = self.default_msg
		return msg

	async def enqueue(self, *msgs: model.Message) -> NoReturn:
		# TODO(rdt17) Consider reverting back to asyncio.gather()
		for m in msgs:
			await self.queue.put(m, block=self.block_queue)

	# await asyncio.gather(*(put(m, block=block) for m in msgs))

	# noinspection PyTypeChecker
	@staticmethod
	def kill() -> NoReturn:
		ray.actor.exit_actor()


# Types
RemoteGraph = Union[ray.ObjectRef, graphs.FactorGraph]
RemoteVariables = Sequence[Union[ray.ObjectRef, _ShareTraceVariablePart]]
RemoteFactors = Sequence[Union[ray.ObjectRef, _ShareTraceFactorPart]]


@attr.s  # Slots not allowed since BeliefPropagation uses slots
class RemoteBeliefPropagation(BeliefPropagation):
	"""A multi-process implementation of BeliefPropagation using Ray."""
	_graph = attr.ib(type=RemoteGraph, init=False, repr=False)
	_variables = attr.ib(type=RemoteVariables, init=False, repr=False)
	_factors = attr.ib(type=RemoteFactors, init=False, repr=False)

	def __attrs_post_init__(self):
		super(RemoteBeliefPropagation, self).__attrs_post_init__()

	def __call__(
			self, factors: Contacts, variables: AllRiskScores) -> RiskScores:
		stdout('-----------START BELIEF PROPAGATION-----------')
		result = self.call(factors, variables)
		stdout('------------END BELIEF PROPAGATION------------')
		return result

	@codetiming.Timer(text='Total duration: {:0.6f} s', logger=stdout)
	def call(
			self,
			factors: Contacts,
			variables: AllRiskScores,
			**kwargs) -> RiskScores:
		self.create_graph(factors, variables)
		with codetiming.Timer(text='Sending msgs: {:0.6f} s', logger=stdout):
			refs = self._initiate_message_passing()
			result = self._get_result(refs)
		self._shutdown()
		return result

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Creating graph: {:0.6f} s', logger=stdout)
	def create_graph(
			self,
			factors: Contacts,
			variables: AllRiskScores,
			**kwargs):
		num_cpus = backend.NUM_CPUS
		num_fstores = int(np.ceil(num_cpus / 2))
		num_vstores = int(np.floor(num_cpus / 2))
		builder = graphs.FactorGraphBuilder(
			impl=self.impl,
			# Graph structure is static
			share_graph=True,
			# Separate the stateless (structure) and stateful (attributes)
			use_vertex_store=True,
			# Store actor index in shared graph for actor-actor communication
			store_in_graph=['address'],
			num_stores=(num_fstores, num_vstores))

		local_msgs = self._add_variables(builder, variables, num_vstores)
		self._add_factors_and_edges(builder, factors)
		self._graph, _, _, (factor_stores, variable_stores) = builder.build()
		self._variables = tuple(
			_ShareTraceVariablePart.remote(
				vertex_store=s,
				local_msgs=m,
				send_threshold=self.send_threshold,
				should_send=self.should_send,
				should_stop=self.should_stop,
				iterations=self.iterations,
				queue_size=self.queue_size)
			for s, m in zip(variable_stores, local_msgs))
		self._factors = tuple(
			_ShareTraceFactorPart.remote(
				vertex_store=s,
				transmission_rate=self.transmission_rate,
				time_buffer=self.time_buffer,
				time_constant=self.time_constant,
				default_msg=self.default_msg,
				queue_size=self.queue_size)
			for s in factor_stores)

	# noinspection PyTypeChecker
	def _add_variables(
			self,
			builder: graphs.FactorGraphBuilder,
			variables: AllRiskScores,
			num_parts: int) -> Iterable[Sequence[model.Message]]:
		vertices = {}
		local = [[] for _ in range(num_parts)]
		for q, (k, v) in enumerate((str(k), v) for k, v in variables):
			v1, v2 = itertools.tee(v)
			vertices[k] = {'local': max(v1, default=self.default_msg)}
			msgs = (model.Message(sender=k, receiver=k, content=c) for c in v2)
			local[q % num_parts].extend(msgs)
		builder.add_variables(vertices)
		return local

	# noinspection PyTypeChecker
	@staticmethod
	def _add_factors_and_edges(
			builder: graphs.FactorGraphBuilder,
			factors: Contacts) -> NoReturn:
		def make_key(factor: model.Contact):
			parts = tuple(str(u) for u in sorted(factor.users))
			key = '_'.join(parts)
			return key, parts

		edges = []
		vertices = {}
		for f in factors:
			k, (v1, v2) = make_key(f)
			edges.extend(((k, v1), (k, v2)))
			occurs = (o.as_array() for o in f.occurrences)
			vertices[k] = {'occurrences': np.array(list(occurs)).flatten()}
		builder.add_factors(vertices)
		builder.add_edges(edges)

	def _initiate_message_passing(self) -> Sequence[ray.ObjectRef]:
		for a in self._factors:
			a.send_to_variables.remote(self._graph, self._variables)
		return [
			a.send_to_factors.remote(self._graph, self._factors)
			for a in self._variables]

	def should_send(
			self,
			local: model.RiskScore,
			incoming: model.RiskScore,
			**kwargs) -> bool:
		return _should_send(
			local, incoming, self.send_condition, self.send_threshold)

	def should_stop(self, epoch: Iterable[numbers.Real], **kwargs) -> bool:
		return sum(epoch) < self.tolerance

	@staticmethod
	def _get_result(refs: Sequence[ray.ObjectRef]) -> RiskScores:
		return itertools.chain.from_iterable(ray.get(refs))

	def _shutdown(self):
		if isinstance(self._graph, backend.Process):
			self._graph.kill()
		for a in itertools.chain(self._factors, self._variables):
			a.kill.remote()


def _should_send(
		local: model.RiskScore,
		incoming: model.RiskScore,
		condition: str,
		threshold: float) -> bool:
	if condition is _SEND_BY_MESSAGE:
		send = incoming.value > threshold
	elif condition is _SEND_BY_LOCAL:
		send = incoming.value > local.value * threshold
	else:
		send = True
	return send

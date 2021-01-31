import abc
import datetime
import functools
import itertools
import numbers
import random
from typing import (
	Any, Callable, Collection, Dict, Hashable, Iterable, NoReturn,
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
		System model:				Dell XPS 15 9560
		Physical memory:			16GB RAM
		Processor:					Intel i7-7700HQ CPU @ 2.8GHz
		Cores (logical/physical):	8/4
	
	Simulation setup:
		Users:						500, 1000
		Scores per user:			14
		Days:						14
		Unique locations:			10
		Graph implementation:		Numpy
		Iterations (local/remote):	4/550
		Tolerance:					1e-6
		Transmission factor:		0.8
		Time constant:				1
		Time buffer:				2 days
		Seed:						12345

	Timing:
		500 users (local/remote):	346/374		(1.08x faster)
		1000 users (local/remote):	1330/788	(0.59x faster)

		Linear interpolation: ~600 users leads to 1:1 performance
"""

# Globals
_TWO_DAYS = np.timedelta64(2, 'D')
_DEFAULT_MESSAGE = model.RiskScore(
	name='DEFAULT_ID', timestamp=datetime.datetime.utcnow(), value=0)
stdout = backend.STDOUT
stderr = backend.STDERR
# Types
TimestampBuffer = Union[datetime.timedelta, np.timedelta64]
RiskScores = Iterable[model.RiskScore]
AllRiskScores = Collection[Tuple[Hashable, RiskScores]]
Contacts = Iterable[model.Contact]
Result = Iterable[Tuple[Hashable, model.RiskScore]]
Maxes = Union[np.ndarray, Iterable[Tuple[graphs.Vertex, model.RiskScore]]]
StoppingCondition = Callable[..., bool]


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
	msg_threshold = attr.ib(
		type=numbers.Real,
		default=0,
		validator=validators.instance_of(numbers.Real),
		converter=float,
		kw_only=True)
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

	@msg_threshold.validator
	def _check_msg_threshold(self, _, value):
		if not 0 <= value <= 1:
			raise ValueError(
				f"'msg_threshold' must be between 0 and 1, inclusive; got "
				f"{value}")

	@abc.abstractmethod
	def call(self, *args, **kwargs):
		pass

	@abc.abstractmethod
	def create_graph(self, *args, **kwargs):
		pass

	@abc.abstractmethod
	def stopping_condition(self, *args, **kwargs) -> bool:
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
			self, *, factors: Contacts, variables: AllRiskScores) -> Result:
		stdout('-----------START BELIEF PROPAGATION-----------')
		result = self.call(factors, variables)
		stdout('------------END BELIEF PROPAGATION------------')
		return result

	@codetiming.Timer(text='Total duration: {:0.6f} s', logger=stdout)
	def call(
			self,
			factors: Contacts,
			variables: AllRiskScores,
			**kwargs) -> Result:
		self.create_graph(factors, variables)
		i, epoch = 1, None
		stop = False
		while not stop:
			stdout(f'-----------Iteration {i}-----------')
			epoch = self._send_to_factors()
			self._send_to_variables()
			stop = self.stopping_condition(i, epoch)
			i += 1
			stdout(f'---------------------------------')
		return ((v, self._variables.get(v, 'max')) for v in self._variables)

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

	def _add_variables(
			self,
			builder: graphs.FactorGraphBuilder,
			variables: AllRiskScores) -> NoReturn:
		vertices = []
		attrs = {}
		for k, v in ((str(k), v) for k, v in variables):
			vertices.append(k)
			v1, v2 = itertools.tee(v)
			attrs[k] = {
				'max': max(v1, default=self.default_msg),
				'inbox': {k: tuple(v2)}}
		builder.add_variables(vertices, attributes=attrs)

	@staticmethod
	def _add_factors_and_edges(
			builder: graphs.FactorGraphBuilder,
			factors: Contacts) -> NoReturn:
		def make_key(factor: model.Contact):
			parts = tuple(str(u) for u in sorted(factor.users))
			key = '_'.join(parts)
			return key, parts

		vertices, edges = [], []
		attrs = {}
		for f in factors:
			k, (v1, v2) = make_key(f)
			vertices.append(k)
			edges.extend(((k, v1), (k, v2)))
			occurrences = (o.as_array() for o in f.occurrences)
			attrs[k] = {
				'occurrences': np.array(list(occurrences)).flatten(),
				'inbox': {}}
		builder.add_factors(vertices, attributes=attrs)
		builder.add_edges(edges)

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Sending to factors: {:0.6f} s', logger=stdout)
	def _send_to_factors(self) -> np.ndarray:
		def update_max(sender, incoming: Dict):
			curr_max = self._variables.get(sender, 'max')
			# First iteration only: messages only from self
			if sender in incoming:
				values = itertools.chain([curr_max], incoming[sender])
			else:
				values = itertools.chain([curr_max], incoming.values())
			if (updated := max(values)) > curr_max:
				difference = updated.value - curr_max.value
				self._variables.put([sender], {sender: {'max': updated}})
			else:
				difference = 0
			return difference

		def send(sender, incoming: Dict):
			for f in self._graph.get_neighbors(sender):
				# First iteration only: messages only from self
				if sender in incoming:
					from_others = (
						o for o in incoming[sender]
						if o.value > self.msg_threshold)
				else:
					from_others = (
						msg for o, msg in incoming.items()
						if o != f and msg.value > self.msg_threshold)
				outgoing = {f: {'inbox': {sender: from_others}}}
				self._factors.put([f], outgoing, merge=True)

		epoch = []
		for v in self._variables:
			inbox = self._variables.get(v, 'inbox')
			diff = update_max(v, inbox)
			epoch.append(diff)
			send(v, inbox)
			self._variables.put([v], {v: {'inbox': {}}})
		return np.array(epoch)

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Sending to variables: {:0.6f} s', logger=stdout)
	def _send_to_variables(self) -> NoReturn:
		for f in self._factors:
			neighbors = tuple(self._graph.get_neighbors(f))
			inbox: Dict = self._factors.get(f, 'inbox')
			for i, n in enumerate(neighbors):
				# Assumes factor vertex has a degree of 2
				receiver = neighbors[not i]
				msg = self._compute_message(f, inbox[n])
				outgoing = {receiver: {'inbox': {f: msg}}}
				self._variables.put([receiver], outgoing, merge=True)
			self._factors.put([f], {f: {'inbox': {}}})

	def _compute_message(
			self,
			factor: graphs.Vertex,
			msg: Iterable[model.RiskScore]) -> model.RiskScore:
		def sec_to_day(s: np.ndarray) -> np.float64:
			return np.divide(np.float64(s), 86400)

		occurrences = self._factors.get(factor, attribute='occurrences')
		most_recent = np.max(occurrences['timestamp'])
		msg = np.array([m.as_array() for m in msg]).flatten()
		if msg.size:
			old = np.where(msg['timestamp'] <= most_recent + self.time_buffer)
			msg = msg[old]
		no_valid_messages = not msg.size
		# TODO(rdt17) Should transmission rate alter message value?
		#  Using as random variable results in very high risk scores
		not_transmitted = np.random.uniform() > self.transmission_rate
		if no_valid_messages:
			msg = self.default_msg
		else:
			# Formats time delta as partial days
			diff = sec_to_day(msg['timestamp'] - most_recent)
			# Weight can only decrease original message value
			np.clip(diff, -np.inf, 0, out=diff)
			msg['value'] *= np.exp(diff / self.time_constant)
			msg['value'] *= self.transmission_rate
			msg.sort(order=['value', 'timestamp', 'name'])
			msg = model.RiskScore.from_array(msg[-1])
		return msg

	def stopping_condition(
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
		'vertex_store',
		'queue',
		'add_to_queue',
		'block_queue',
		'msg_threshold',
		'iterations',
		'epoch',
		'stopping_condition']

	def __init__(
			self,
			vertex_store: stores.VertexStore,
			queue_size: int,
			add_to_queue: Sequence[model.Message],
			msg_threshold: float,
			iterations: int,
			stopping_condition: StoppingCondition):
		self.vertex_store = vertex_store
		self.block_queue = queue_size > 0
		self.queue = stores.queue_factory(
			max_size=queue_size, asynchronous=True, local_mode=True)
		self.add_to_queue = add_to_queue
		self.msg_threshold = msg_threshold
		self.iterations = iterations
		self.epoch = []
		self.stopping_condition = stopping_condition

	async def send_to_factors(
			self,
			graph: graphs.FactorGraph,
			factors: Sequence['_ShareTraceFactorPart']) -> Result:
		def update_max(variable, maximum, incoming):
			if (updated := max(incoming, maximum)) > maximum:
				difference = updated.value - maximum.value
				self.vertex_store.put([variable], {variable: {'max': updated}})
			else:
				difference = 0
			return difference

		async def send(from_, to_, message):
			# Avoid self-bias: do not send message back to sending factor
			for f in (n for n in graph.get_neighbors(from_) if n != to_):
				m = model.Message(sender=from_, receiver=f, content=message)
				factor = factors[graph.get_vertex_attr(f, key='address')]
				await factor.enqueue.remote(m)

		# Queues must have enough capacity to hold local messages
		await self.queue.put(*self.add_to_queue, block=False)
		self.add_to_queue = None
		get_max = functools.partial(lambda v: self.vertex_store.get(v, 'max'))
		stop = False
		while not stop:
			for _ in range(self.iterations):
				msg = await self.queue.get(block=True)
				# Receiver becomes the sender and vice versa
				sender, receiver, msg = msg.receiver, msg.sender, msg.content
				curr_max = get_max(sender)
				diff = update_max(sender, curr_max, msg)
				self.epoch.append(diff)
				if msg.value > self.msg_threshold:
					await send(sender, receiver, msg)
			stop = self.stopping_condition(self.epoch)
			self.epoch.clear()
		return np.array(list((v, get_max(v)) for v in self.vertex_store))

	async def enqueue(self, *messages: model.Message) -> NoReturn:
		for m in messages:
			await self.queue.put(m, block=self.block_queue)

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
		'msg_threshold',
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
			*,
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

	def _compute_message(
			self,
			factor: graphs.Vertex,
			msg: model.RiskScore) -> model.RiskScore:
		def sec_to_day(s: np.ndarray) -> np.float64:
			return np.divide(np.float64(s), 86400)

		occurrences = self.vertex_store.get(factor, attribute='occurrences')
		msg = msg.as_array()
		most_recent = np.max(occurrences['timestamp'])
		after_contact = msg['timestamp'] < most_recent + self.time_buffer
		# TODO(rdt17) Should transmission rate alter message value?
		#  Using as random variable results in very high risk scores
		not_transmitted = np.random.uniform() > self.transmission_rate
		if after_contact:
			msg = self.default_msg
		else:
			# Formats time delta as partial days
			diff = sec_to_day(msg['timestamp'] - most_recent)
			# Weight can only decrease original message value
			np.clip(diff, -np.inf, 0, out=diff)
			msg['value'] *= np.exp(diff / self.time_constant)
			msg['value'] *= self.transmission_rate
			msg = model.RiskScore.from_array(msg)
		return msg

	async def enqueue(self, *messages: model.Message) -> NoReturn:
		for m in messages:
			await self.queue.put(m, block=self.block_queue)

	@staticmethod
	def kill() -> NoReturn:
		ray.actor.exit_actor()


# Types
RemoteGraph = Union[ray.ObjectRef, graphs.FactorGraph]
RemoteVariables = Sequence[Union[ray.ObjectRef, _ShareTraceVariablePart]]
RemoteFactors = Sequence[Union[ray.ObjectRef, _ShareTraceFactorPart]]


@attr.s  # Can't use slots since it inherits from slots
class RemoteBeliefPropagation(BeliefPropagation):
	"""A multi-process implementation of BeliefPropagation using Ray."""
	_graph = attr.ib(type=RemoteGraph, init=False, repr=False)
	_variables = attr.ib(type=RemoteVariables, init=False, repr=False)
	_factors = attr.ib(type=RemoteFactors, init=False, repr=False)

	def __attrs_post_init__(self):
		super(RemoteBeliefPropagation, self).__attrs_post_init__()

	def __call__(
			self, *, factors: Contacts, variables: AllRiskScores) -> Result:
		stdout('-----------START BELIEF PROPAGATION-----------')
		result = self.call(factors, variables)
		stdout('------------END BELIEF PROPAGATION------------')
		return result

	@codetiming.Timer(text='Total duration: {:0.6f} s', logger=stdout)
	def call(
			self,
			factors: Contacts,
			variables: AllRiskScores,
			**kwargs) -> Result:
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
		def num_stores():
			# < 1001 variables: 1 CPU
			v_stores = max(1, np.ceil(np.log10(len(variables))) - 2)
			# 2:1 factor:variable ratio
			f_stores = max(backend.NUM_CPUS - v_stores, 2 * v_stores)
			return int(f_stores), int(v_stores)

		num_fstores, num_vstores = num_stores()
		builder = graphs.FactorGraphBuilder(
			impl=self.impl,
			# Graph structure is static
			share_graph=True,
			graph_as_actor=False,
			# Separate the stateless (structure) and stateful (attributes)
			use_vertex_store=True,
			# Store actor index in shared graph for actor-actor communication
			store_in_graph=['address'],
			num_stores=(num_fstores, num_vstores),
			# Vertex stores are stored in each graph partition actor
			store_as_actor=False,
			# Prevent actors from being killed automatically
			detached=True)

		add_to_queues = self._add_variables(builder, variables, num_vstores)
		self._add_factors_and_edges(builder, factors)
		self._graph, _, _, (factor_stores, variable_stores) = builder.build()
		self._variables = tuple(
			_ShareTraceVariablePart.remote(
				vertex_store=s,
				add_to_queue=q,
				msg_threshold=self.msg_threshold,
				stopping_condition=self.stopping_condition,
				iterations=self.iterations,
				queue_size=self.queue_size)
			for s, q in zip(variable_stores, add_to_queues))
		self._factors = tuple(
			_ShareTraceFactorPart.remote(
				vertex_store=s,
				transmission_rate=self.transmission_rate,
				time_buffer=self.time_buffer,
				time_constant=self.time_constant,
				default_msg=self.default_msg,
				queue_size=self.queue_size)
			for s in factor_stores)

	def _add_variables(
			self,
			builder: graphs.FactorGraphBuilder,
			variables: AllRiskScores,
			num_queues: int) -> Iterable[Sequence[model.Message]]:
		vertices = []
		attrs = {}
		queues = [[] for _ in range(num_queues)]
		for q, (k, v) in enumerate((str(k), v) for k, v in variables):
			vertices.append(k)
			v1, v2 = itertools.tee(v)
			attrs[k] = {'max': max(v1, default=self.default_msg)}
			msgs = (model.Message(sender=k, receiver=k, content=c) for c in v2)
			queues[q % num_queues].extend(msgs)
		builder.add_variables(vertices, attributes=attrs)
		return queues

	@staticmethod
	def _add_factors_and_edges(
			builder: graphs.FactorGraphBuilder,
			factors: Contacts) -> NoReturn:
		def make_key(factor: model.Contact):
			parts = tuple(str(u) for u in sorted(factor.users))
			key = '_'.join(parts)
			return key, parts

		vertices, edges = [], []
		attrs = {}
		for f in factors:
			k, (v1, v2) = make_key(f)
			vertices.append(k)
			edges.extend(((k, v1), (k, v2)))
			occurrences = (o.as_array() for o in f.occurrences)
			attrs[k] = {'occurrences': np.array(list(occurrences)).flatten()}
		builder.add_factors(vertices, attributes=attrs)
		builder.add_edges(edges)

	def _initiate_message_passing(self) -> Sequence[ray.ObjectRef]:
		for a in self._factors:
			a.send_to_variables.remote(
				graph=self._graph, variables=self._variables)
		return [
			a.send_to_factors.remote(graph=self._graph, factors=self._factors)
			for a in self._variables]

	@staticmethod
	def _get_result(refs: Sequence[ray.ObjectRef]) -> Result:
		result = itertools.chain.from_iterable(ray.get(refs))
		return tuple(result)

	def _shutdown(self):
		if isinstance(self._graph, backend.ActorMixin):
			self._graph.kill()
		for a in itertools.chain(self._factors, self._variables):
			a.kill.remote()

	def stopping_condition(self, epoch: np.ndarray, **kwargs) -> bool:
		return sum(epoch) < self.tolerance

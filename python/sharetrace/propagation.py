import abc
import datetime
import functools
import itertools
import random
from typing import (
	Any, Callable, Collection, Hashable, Iterable, NoReturn, Optional,
	Sequence, Tuple, Union)

import codetiming
import numpy as np
import ray

import backend
import graphs
import model
import stores

_TWO_DAYS = np.timedelta64(2, 'D')
_DEFAULT_MESSAGE = model.RiskScore(
	name='DEFAULT_ID', timestamp=backend.TIME, value=0)
RiskScores = Iterable[model.RiskScore]
Maxes = Union[np.ndarray, Iterable[Tuple[graphs.Vertex, model.RiskScore]]]
TimestampBuffer = Union[datetime.timedelta, np.timedelta64]
AllRiskScores = Collection[Tuple[Hashable, RiskScores]]
Contacts = Iterable[model.Contact]
Vertices = Union[np.ndarray, ray.ObjectRef]
EpochHistory = Sequence[float]
StoppingCondition = Callable[[EpochHistory], bool]
OptionalObjectRefs = Optional[Sequence[ray.ObjectRef]]
Result = Iterable[Tuple[graphs.Vertex, model.RiskScore]]
stdout = backend.STDOUT
stderr = backend.STDERR


class BeliefPropagation:
	"""Runs the belief propagation algorithm to compute exposure scores.

	Notes:
		A factor vertex represents a pair of people, with vertex data
		containing all occurrences (time-duration pairs) in the recent past
		that the two individuals came into sufficiently long contact. A
		variable vertex represents an individual, with vertex data containing
		the local risk scores that are derived from user-reported symptoms,
		the maximum of these scores, and all risk scores sent from neighboring
		factor vertices.

		Following the core message-passing principle of belief propagation, the
		algorithm performs iterative computation between the factor and
		variable vertex sets. The algorithm begins with all variable vertices
		selecting their maximum local score and sending it to all of their
		neighboring factor vertices. Then, all factor vertices filter the
		risk scores, based on when the individuals came into contact. That
		is, only risk scores recorded prior to at least one occurrence that
		defines the contact are retained for subsequent calculation. A
		weighted transformation that accounts for the amount of time passed
		from when the risk score was recorded and the time of running the
		algorithm is applied to all risk scores from a variable vertex. The
		maximum of these is sent to the other variable vertex connected to
		the factor vertex.

		This completes one iteration of the algorithm and is repeated until
		either a certain number of iterations has passed or the summed
		difference in maximum variable risk scores from the previous
		iteration drops below a set tolerance, whichever condition is
		satisfied first.

	Attributes:
		# TODO Update
		transmission_rate: The scaling applied to each weighted risk score at
			the factor vertex. A value of 1 implies that only the effect of
			the exponential time decay weighting scheme affects which risk
			score is sent to the opposite variable vertex.

		# TODO Update
		tolerance: The minimum value that must be exceeded to continue onto
			the next iteration. The summed difference of all variable
			vertices' max risk scores between the previous iteration and
			current iteration must exceed the tolerance value to continue
			onto the next iteration.

		# TODO Update
		iterations: The maximum number of loops the algorithm will run. If
			the tolerance value is not exceeded, then it may not be the case
			that all iterations are completed.

		time_buffer: The amount of time (in seconds) that must have
			passed between an occurrence and risk score for it to be retained
			by the factor vertex during computation.

		queue_size: Maximum number of messages allowed to be passed
			between vertex sets. If left unspecified, no limit is set.

		impl: Implementation to use for the factor graph.

		seed: Random seed to allow for reproducibility. If left unset,
			the current seed is used.

		local_mode: If True, a single-process is used. Otherwise, the Ray
			library is utilized to parallelize computation for each vertex
			set.
	"""

	__slots__ = [
		'transmission_rate',
		'tolerance',
		'iterations',
		'time_buffer',
		'msg_threshold',
		'time_constant',
		'queue_size',
		'impl',
		'seed',
		'local_mode',
		'_graph',
		'_factor_actors',
		'_variable_actors',
		'_block_queue']

	def __init__(
			self,
			*,
			transmission_rate: float = 0.8,
			tolerance: float = 1e-6,
			iterations: int = 1000,
			time_buffer: TimestampBuffer = _TWO_DAYS,
			time_constant: float = 1,
			msg_threshold: float = 0,
			queue_size: int = 0,
			impl: str = graphs.DEFAULT,
			local_mode: bool = None,
			seed: Any = None):
		self._check_tolerance(tolerance)
		self.transmission_rate = float(transmission_rate)
		self.tolerance = float(tolerance)
		self.iterations = int(iterations)
		self.time_buffer = np.timedelta64(time_buffer, 's')
		self.msg_threshold = float(msg_threshold)
		self.time_constant = float(time_constant)
		self.queue_size = int(queue_size)
		self._block_queue = self.queue_size > 0
		self.impl = str(impl)
		local_mode = backend.LOCAL_MODE if local_mode is None else local_mode
		self.local_mode = bool(local_mode)
		self.seed = seed
		if self.seed is not None:
			random.seed(self.seed)
			np.random.seed(self.seed)

	@staticmethod
	def _check_tolerance(value):
		if value <= 0:
			raise ValueError("'tolerance' must be greater than 0")

	def __repr__(self):
		return backend.rep(
			self.__class__.__name__,
			transmission_rate=self.transmission_rate,
			tolerance=self.tolerance,
			iterations=self.iterations,
			time_buffer=self.time_buffer,
			queue_size=self.queue_size,
			impl=self.impl,
			local_mode=self.local_mode,
			seed=self.seed)

	def __call__(
			self, *, factors: Contacts, variables: AllRiskScores) -> Result:
		stdout('-----------START BELIEF PROPAGATION-----------')
		result = self._call(factors, variables)
		stdout('------------END BELIEF PROPAGATION------------')
		return result

	@codetiming.Timer(text='Total duration: {:0.6f} s', logger=stdout)
	def _call(self, factors, variables) -> Result:
		self._create_graph(factors, variables)
		remaining = self._initiate_message_passing()
		maxes = self._collect_results(remaining)
		print(maxes)
		self._shutdown()
		return maxes

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Creating graph: {:0.6f} s', logger=stdout)
	def _create_graph(self, factors, variables) -> NoReturn:
		def num_stores():
			if self.local_mode:
				f_stores, v_stores = 1, 1
			else:
				# < 1001 variables: 1 CPU
				v_stores = max(1, np.ceil(np.log10(len(variables))) - 2)
				# 2:1 factor:variable ratio
				f_stores = max(self._get_num_cpus() - v_stores, 2 * v_stores)
			return int(f_stores), int(v_stores)

		num_factor_stores, num_variable_stores = num_stores()
		builder = graphs.FactorGraphBuilder(
			impl=self.impl,
			# Graph structure is static; local mode = single-process
			share_graph=not self.local_mode,
			graph_as_actor=False,
			# Separate the stateless (structure) and stateful (attributes)
			use_vertex_store=True,
			# Store actor index in shared graph for actor-actor communication
			store_in_graph=['address'],
			num_stores=(num_factor_stores, num_variable_stores),
			# Vertex stores are stored in each actor
			store_as_actor=False,
			# Prevent actors from being killed automatically
			detached=True)
		queue = functools.partial(
			stores.queue_factory,
			self.queue_size,
			asynchronous=False,
			local_mode=True)
		variable_queues = tuple(queue() for _ in range(num_variable_stores))
		factor_queues = tuple(queue() for _ in range(num_factor_stores))
		self._add_variables(builder, variables, variable_queues)
		self._add_factors_and_edges(builder, factors)
		self._graph, _, _, (factor_stores, variable_stores) = builder.build()
		kwargs = {'queue_size': self.queue_size, 'local_mode': self.local_mode}
		self._variable_actors = tuple(
			ShareTraceVariablePart(
				vertex_store=s,
				queue=q,
				msg_threshold=self.msg_threshold,
				stopping_condition=self._stopping_condition,
				iterations=self.iterations,
				**kwargs)
			for s, q in zip(variable_stores, variable_queues))
		self._factor_actors = tuple(
			ShareTraceFactorPart(
				vertex_store=s,
				queue=q,
				transmission_rate=self.transmission_rate,
				time_buffer=self.time_buffer,
				time_constant=self.time_constant,
				default_msg=_DEFAULT_MESSAGE,
				**kwargs)
			for s, q in zip(factor_stores, factor_queues))

	@staticmethod
	def _add_variables(
			builder: graphs.FactorGraphBuilder,
			variables: AllRiskScores,
			queues: Sequence[stores.Queue]) -> NoReturn:
		vertices = []
		attrs = {}
		num_queues = len(queues)
		for q, (k, v) in enumerate((str(k), v) for k, v in variables):
			vertices.append(k)
			v1, v2 = itertools.tee(v)
			attrs.update({k: {'max': max(v1, default=_DEFAULT_MESSAGE)}})
			msgs = (model.Message(sender=k, receiver=k, content=c) for c in v2)
			for m in msgs:
				# Queues must have enough capacity to hold local messages
				queues[q % num_queues].put(m, block=False)
		for v in attrs:
			print(attrs[v]['max'])
		print('------------------')
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
			attrs[k] = {'occurrences': f.occurrences}
		builder.add_factors(vertices, attributes=attrs)
		builder.add_edges(edges)

	def _stopping_condition(self, history: EpochHistory):
		return sum(history) < self.tolerance

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Sending to factors: {:0.6f} s', logger=stdout)
	def _initiate_message_passing(self) -> Sequence[ray.ObjectRef]:
		kwargs = {'graph': self._graph, 'factors': self._factor_actors}
		return [a.send_to_factors(**kwargs) for a in self._variable_actors]

	def _collect_results(self, refs: Sequence) -> np.ndarray:
		if self.local_mode:
			result = refs[0]
		else:
			result = itertools.chain.from_iterable(ray.get(refs))
			result = np.array(list(result))
		return result

	def _shutdown(self):
		if not self.local_mode:
			if isinstance(self._graph, backend.ActorMixin):
				self._graph.kill()
			for a in self._variable_actors:
				a.kill()
			for a in self._factor_actors:
				a.kill()

	def _get_num_cpus(self) -> int:
		return 1 if self.local_mode else backend.NUM_CPUS


class ShareTraceFGPart(backend.ActorMixin):
	__slots__ = []

	def __init__(self):
		super(ShareTraceFGPart, self).__init__()

	@abc.abstractmethod
	def enqueue(self, *messages: model.Message) -> NoReturn:
		pass


class ShareTraceVariablePart(graphs.VariablePart, ShareTraceFGPart):
	__slots__ = ['local_mode', '_actor']

	def __init__(
			self,
			*,
			vertex_store: stores.VertexStore,
			stopping_condition: StoppingCondition,
			queue: stores.Queue = None,
			queue_size: int = 0,
			msg_threshold: float = 0,
			iterations: int = 1000,
			local_mode: bool = None):
		super(ShareTraceVariablePart, self).__init__()
		kwargs = {
			'vertex_store': vertex_store,
			'stopping_condition': stopping_condition,
			'queue': queue,
			'queue_size': queue_size,
			'msg_threshold': msg_threshold,
			'iterations': iterations}
		local_mode = backend.LOCAL_MODE if local_mode is None else local_mode
		self.local_mode = bool(local_mode)
		if local_mode:
			self._actor = _ShareTraceVariablePart(**kwargs)
		else:
			self._actor = ray.remote(_ShareTraceVariablePart).remote(**kwargs)

	def send_to_factors(
			self,
			*,
			graph: graphs.FactorGraph,
			factors: Sequence['ShareTraceFactorPart']) -> NoReturn:
		kwargs = {'graph': graph, 'factors': factors}
		func = self._actor.send_to_factors
		func(**kwargs) if self.local_mode else func.remote(**kwargs)

	def enqueue(self, *messages: model.Message) -> NoReturn:
		func = self._actor.enqueue
		func(*messages) if self.local_mode else func.remote(*messages)

	def kill(self) -> NoReturn:
		if not self.local_mode:
			ray.kill(self._actor)


class _ShareTraceVariablePart(graphs.VariablePart, ShareTraceFGPart):
	__slots__ = [
		'vertex_store',
		'queue',
		'block_queue',
		'msg_threshold',
		'iterations',
		'epoch_history',
		'stopping_condition'
	]

	def __init__(
			self,
			*,
			vertex_store: stores.VertexStore,
			queue: stores.Queue,
			queue_size: int,
			msg_threshold: float,
			iterations: int,
			stopping_condition: StoppingCondition):
		super(_ShareTraceVariablePart, self).__init__()
		self._check_queue_and_queue_size(queue, queue_size)
		self._check_msg_threshold(msg_threshold)
		self._check_iterations(iterations)
		self.vertex_store = vertex_store
		if queue_size is None:
			queue_size = queue.max_size
		self.block_queue = queue_size > 0
		if queue is None:
			self.queue = stores.queue_factory(
				max_size=queue_size, asynchronous=False, local_mode=True)
		else:
			self.queue = queue
		self.msg_threshold = float(msg_threshold)
		self.iterations = int(iterations)
		self.epoch_history = []
		self.stopping_condition = stopping_condition

	@staticmethod
	def _check_queue_and_queue_size(queue, queue_size):
		# TODO(rdt17) Issue warning if both are not None
		if queue is None and queue_size is None:
			raise ValueError("Must provide either 'queue' or 'queue_size'")

	@staticmethod
	def _check_msg_threshold(value):
		if not 0 <= value <= 1:
			raise ValueError(
				"'msg_threshold' must be between 0 and 1, inclusive")

	@staticmethod
	def _check_iterations(value):
		if value < 1:
			raise ValueError("'iterations' must be at least 1")

	def send_to_factors(
			self,
			*,
			graph: graphs.FactorGraph,
			factors: Sequence['ShareTraceFactorPart']) -> np.ndarray:
		def update_max(variable, maximum, incoming):
			if (updated := max(incoming, maximum)) > maximum:
				delta = updated.value - maximum.value
				updated = {variable: {'max': updated}}
				self.vertex_store.put(keys=[variable], attributes=updated)
			else:
				delta = 0
			return delta

		def send(from_, to_, message):
			# Avoid self-bias: do not send message back to sending factor
			for f in (n for n in graph.get_neighbors(from_) if n != to_):
				m = model.Message(sender=from_, receiver=f, content=message)
				factor = factors[graph.get_vertex_attr(f, key='address')]
				factor.enqueue(m)

		get_max = functools.partial(self.vertex_store.get, attribute='max')
		should_stop = False
		while not should_stop:
			for i in range(self.iterations):
				msg = self.queue.get(block=True)
				# Receiver becomes the sender and vice versa
				sender, receiver, msg = msg.receiver, msg.sender, msg.content
				curr_max = get_max(sender)
				diff = update_max(sender, curr_max, msg)
				self.epoch_history.append(diff)
				if msg.value > self.msg_threshold:
					send(sender, receiver, msg)
			should_stop = self.stopping_condition(self.epoch_history)
			self.epoch_history.clear()
		return np.array([get_max(v) for v in self.vertex_store])

	def enqueue(self, *messages: model.Message) -> NoReturn:
		for m in messages:
			self.queue.put(m, block=self.block_queue)

	def kill(self) -> NoReturn:
		pass


class ShareTraceFactorPart(graphs.FactorPart, ShareTraceFGPart):
	__slots__ = ['local_mode', '_actor']

	def __init__(
			self,
			*,
			vertex_store: stores.VertexStore,
			transmission_rate: float = 0.8,
			time_buffer: np.timedelta64 = _TWO_DAYS,
			time_constant: float = 1,
			queue: stores.Queue = None,
			queue_size: int = 0,
			default_msg: model.RiskScore = _DEFAULT_MESSAGE,
			local_mode: bool = None):
		super(ShareTraceFactorPart, self).__init__()
		kwargs = {
			'vertex_store': vertex_store,
			'transmission_rate': transmission_rate,
			'time_buffer': time_buffer,
			'time_constant': time_constant,
			'queue': queue,
			'queue_size': queue_size,
			'default_msg': default_msg}
		local_mode = backend.LOCAL_MODE if local_mode is None else local_mode
		self.local_mode = bool(local_mode)
		if local_mode:
			self._actor = _ShareTraceFactorPart(**kwargs)
		else:
			self._actor = ray.remote(_ShareTraceFactorPart).remote(**kwargs)

	def send_to_variables(
			self,
			*,
			graph: graphs.FactorGraph,
			variables: Sequence[ShareTraceVariablePart]) -> NoReturn:
		kwargs = {'graph': graph, 'variables': variables}
		func = self._actor.send_to_variables
		func(**kwargs) if self.local_mode else func.remote(**kwargs)

	def enqueue(self, *messages: model.Message) -> NoReturn:
		func = self._actor.enqueue
		func(*messages) if self.local_mode else func.remote(*messages)

	def kill(self) -> NoReturn:
		if not self.local_mode:
			ray.kill(self._actor)


class _ShareTraceFactorPart(graphs.FactorPart, ShareTraceFGPart):
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
			*,
			vertex_store: stores.VertexStore,
			queue: stores.Queue,
			queue_size: int,
			transmission_rate: float,
			time_buffer: np.timedelta64,
			time_constant: float,
			default_msg: model.RiskScore):
		super(_ShareTraceFactorPart, self).__init__()
		self._check_queue_and_queue_size(queue, queue_size)
		self._check_transmission_rate(transmission_rate)
		self.vertex_store = vertex_store
		self.block_queue = queue_size > 0
		if queue is None:
			self.queue = stores.queue_factory(
				max_size=queue_size, asynchronous=False, local_mode=True)
		else:
			self.queue = queue
		self.transmission_rate = float(transmission_rate)
		self.time_buffer = np.timedelta64(time_buffer, 's')
		self.time_constant = float(time_constant)
		self.default_msg = default_msg

	@staticmethod
	def _check_queue_and_queue_size(queue, queue_size):
		# TODO(rdt17) Issue warning if both are not None
		if queue is None and queue_size is None:
			raise ValueError("Must provide either 'queue' or 'queue_size'")

	@staticmethod
	def _check_transmission_rate(value):
		if value < 0 or value > 1:
			raise ValueError(
				"'transmission_rate' must be between 0 and 1, inclusive")

	def send_to_variables(
			self,
			*,
			graph: graphs.FactorGraph,
			variables: Sequence[ShareTraceVariablePart]) -> NoReturn:
		while True:
			msg = self.queue.get(block=True)
			# Receiver becomes the sender
			sender = msg.receiver
			neighbors = tuple(graph.get_neighbors(sender))
			# Assumes factor vertex has a degree of 2
			receiver = neighbors[not neighbors.index(msg.sender)]
			msg = self._compute_message(factor=sender, msg=msg)
			msg = model.Message(sender=sender, receiver=receiver, content=msg)
			v = graph.get_vertex_attr(receiver, key='address')
			variables[v].enqueue(msg)

	def _compute_message(
			self,
			factor: graphs.Vertex,
			msg: model.RiskScore) -> model.RiskScore:
		def sec_to_day(s: np.float64) -> np.float64:
			return np.divide(s, 86400)

		occurrences = self.vertex_store.get(factor, attribute='occurrences')
		occurrences = np.array([o.as_array() for o in occurrences]).flatten()
		msg = msg.as_array()
		most_recent = np.max(occurrences['timestamp'])
		after_contact = msg['timestamp'] > most_recent + self.time_buffer
		not_transmitted = np.random.uniform() > self.transmission_rate
		if after_contact or not_transmitted:
			msg = self.default_msg
		else:
			# Formats time delta as partial days
			diff = sec_to_day(np.float64(msg['timestamp'] - most_recent))
			# Weight can only decrease original message value
			np.clip(diff, -np.inf, 0, out=diff)
			msg['value'] *= np.exp(diff / self.time_constant)
			msg = model.RiskScore.from_array(msg)
		return msg

	def enqueue(self, *messages: model.Message) -> NoReturn:
		for m in messages:
			self.queue.put(m, block=self.block_queue)

	def kill(self) -> NoReturn:
		pass

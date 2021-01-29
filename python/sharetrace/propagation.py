import abc
import datetime
import functools
import itertools
import random
from typing import (
	Any, Collection, Hashable, Iterable, NoReturn, Optional, Sequence, Tuple,
	Union)

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
		transmission_rate: The scaling applied to each weighted risk score at
			the factor vertex. A value of 1 implies that only the effect of
			the exponential time decay weighting scheme affects which risk
			score is sent to the opposite variable vertex.

		tolerance: The minimum value that must be exceeded to continue onto
			the next iteration. The summed difference of all variable
			vertices' max risk scores between the previous iteration and
			current iteration must exceed the tolerance value to continue
			onto the next iteration.

		iterations: The maximum number of loops the algorithm will run. If
			the tolerance value is not exceeded, then it may not be the case
			that all iterations are completed.

		time_buffer: The amount of time (in seconds) that must have
			passed between an occurrence and risk score for it to be retained
			by the factor vertex during computation.

		max_queue_size: Maximum number of messages allowed to be passed
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
		'max_queue_size',
		'impl',
		'seed',
		'local_mode',
		'_graph',
		'_factor_actors',
		'_variable_actors']

	def __init__(
			self,
			*,
			transmission_rate: float = 0.8,
			tolerance: float = 1e-6,
			iterations: int = 4,
			time_buffer: TimestampBuffer = _TWO_DAYS,
			time_constant: float = 1,
			msg_threshold: float = 0,
			max_queue_size: int = 0,
			impl: str = graphs.DEFAULT,
			local_mode: bool = None,
			seed: Any = None):
		self._check_tolerance(tolerance)
		self._check_iterations(iterations)
		self.transmission_rate = float(transmission_rate)
		self.tolerance = float(tolerance)
		self.iterations = int(iterations)
		self.time_buffer = np.timedelta64(time_buffer, 's')
		self.msg_threshold = float(msg_threshold)
		self.time_constant = float(time_constant)
		self.max_queue_size = int(max_queue_size)
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

	@staticmethod
	def _check_iterations(value):
		if value < 1:
			raise ValueError("'iterations' must be at least 1")

	def __repr__(self):
		return backend.rep(
			self.__class__.__name__,
			transmission_rate=self.transmission_rate,
			tolerance=self.tolerance,
			iterations=self.iterations,
			time_buffer=self.time_buffer,
			max_queue_size=self.max_queue_size,
			impl=self.impl,
			local_mode=self.local_mode,
			seed=self.seed)

	def __call__(
			self, *, factors: Contacts, variables: AllRiskScores) -> Result:
		stdout('-----------START BELIEF PROPAGATION-----------')
		result = self._call(factors, variables)
		stdout('------------END BELIEF PROPAGATION------------')
		return result

	# TODO Base on number of messages passed as "epoch"
	@codetiming.Timer(text='Total duration: {:0.6f} s', logger=stdout)
	def _call(self, factors, variables) -> Result:
		self._create_graph(factors, variables)
		maxes = self._get_maxes(only_value=True)
		i, t = 0, np.inf
		while i < self.iterations and t > self.tolerance:
			stdout(f'-----------Iteration {i + 1}-----------')
			remaining = self._send_to_factors()
			remaining = self._send_to_variables()
			i, t, maxes = self._update(iteration=i, maxes=maxes)
			stdout(f'---------------------------------')
		maxes = self._get_maxes(with_variable=True)
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
			stores.queue_factory, self.max_queue_size, local_mode=True)
		variable_queues = [queue() for _ in range(num_variable_stores)]
		factor_queues = [queue() for _ in range(num_factor_stores)]
		self._add_variables(builder, variables, variable_queues)
		self._add_factors_and_edges(builder, factors)
		# TODO Do we need factors and variables?
		graph, factors, variables, vertex_stores = builder.build()
		factor_stores, variable_stores = vertex_stores
		self._graph = graph
		self._variable_actors = None
		self._factor_actors = None

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
				queues[q % num_queues].put(m)
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

	def _get_maxes(
			self,
			*,
			only_value: bool = False,
			with_variable: bool = False
	) -> Union[np.ndarray, Iterable[Tuple[graphs.Vertex, model.RiskScore]]]:
		kwargs = {'only_value': only_value, 'with_variable': with_variable}
		maxes = [a.get_maxes(**kwargs) for a in self._actors]
		return self._collect_results(maxes)

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Sending to factors: {:0.6f} s', logger=stdout)
	def _send_to_factors(self) -> Sequence:
		kwargs = {'graph': self._graph, 'queue': self._queue}
		return [a.send_to_factors(**kwargs) for a in self._actors]

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Sending to variables: {:0.6f} s', logger=stdout)
	def _send_to_variables(self) -> Sequence:
		kwargs = {
			'graph': self._graph,
			'queue': self._queue,
			'transmission_rate': self.transmission_rate,
			'buffer': self.time_buffer}
		return [a.send_to_variables(**kwargs) for a in self._actors]

	def _update(
			self,
			iteration: int,
			maxes: np.ndarray) -> Tuple[int, float, np.ndarray]:
		if iteration:  # Maxes aren't updated until after the first iteration
			iter_maxes = self._get_maxes(only_value=True)
			tolerance = np.float64(np.sum(iter_maxes - maxes))
		else:
			tolerance = np.inf
			iter_maxes = maxes
		stdout(f'Tolerance: {np.round(tolerance, 6)}')
		return iteration + 1, tolerance, iter_maxes

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
			actors = itertools.chain(
				self._variable_actors, self._factor_actors)
			for a in actors:
				a.kill()

	def _get_num_cpus(self) -> int:
		return 1 if self.local_mode else backend.NUM_CPUS


class ShareTraceFGPart(backend.ActorMixin):
	__slots__ = []

	def __init__(self):
		super(ShareTraceFGPart, self).__init__()

	@abc.abstractmethod
	def enqueue(self, *messages: model.Message) -> bool:
		pass


class ShareTraceVariablePart(graphs.VariablePart, ShareTraceFGPart):
	__slots__ = ['local_mode', '_actor']

	def __init__(
			self,
			*,
			vertex_store: stores.VertexStore,
			queue: stores.Queue = None,
			max_queue_size: int = 0,
			local_mode: bool = None):
		super(ShareTraceVariablePart, self).__init__()
		kwargs = {
			'vertex_store': vertex_store,
			'queue': queue,
			'max_queue_size': max_queue_size}
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
			factors: Sequence['ShareTraceFactorPart']) -> bool:
		kwargs = {'graph': graph, 'factors': factors}
		func = self._actor.send_to_factors
		return func(**kwargs) if self.local_mode else func.remote(**kwargs)

	def enqueue(self, *messages: model.Message) -> bool:
		func = self._actor.enqueue
		return func(*messages) if self.local_mode else func.remote(*messages)

	def kill(self) -> NoReturn:
		if not self.local_mode:
			ray.kill(self._actor)


class _ShareTraceVariablePart(graphs.VariablePart, ShareTraceFGPart):
	__slots__ = ['vertex_store', 'queue', 'block_queue', 'msg_threshold']

	def __init__(
			self,
			*,
			vertex_store: stores.VertexStore,
			queue: stores.Queue,
			max_queue_size: int,
			msg_threshold: float):
		super(_ShareTraceVariablePart, self).__init__()
		self._check_queue_and_max_queue_size(queue, max_queue_size)
		self._check_msg_threshold(msg_threshold)
		self.vertex_store = vertex_store
		if max_queue_size is None:
			max_queue_size = queue.max_size
		self.block_queue = max_queue_size < 1
		if queue is None:
			self.queue = stores.queue_factory(
				max_size=max_queue_size, local_mode=True)
		else:
			self.queue = queue
		self.msg_threshold = float(msg_threshold)

	@staticmethod
	def _check_queue_and_max_queue_size(queue, max_queue_size):
		# TODO(rdt17) Issue warning if both are not None
		if queue is None and max_queue_size is None:
			raise ValueError("Must provide either 'queue' or 'max_queue_size'")

	@staticmethod
	def _check_msg_threshold(value):
		if not 0 <= value <= 1:
			raise ValueError(
				"'msg_threshold' must be between 0 and 1, inclusive")

	def send_to_factors(
			self,
			*,
			graph: graphs.FactorGraph,
			factors: Sequence['ShareTraceFactorPart']) -> bool:
		def update_max(receiver, mx, incoming):
			if (updated := max(incoming, mx)) > mx:
				updated = {receiver: {'max': updated}}
				self.vertex_store.put(keys=[receiver], attributes=updated)

		def send(sender, receiver, content):
			# Avoid self-bias: do not send message back to sending factor
			for f in (n for n in graph.get_neighbors(receiver) if n != sender):
				m = model.Message(sender=receiver, receiver=f, content=content)
				factor = factors[graph.get_vertex_attr(f, key='address')]
				factor.enqueue(m)

		# TODO Modify loop condition? Set number of messages to process?
		while len(self.queue):
			msg = self.queue.get(block=self.block_queue)
			curr_max = self.vertex_store.get(key=msg.receiver, attribute='max')
			update_max(msg.receiver, curr_max, msg.content)
			if msg.value >= self.msg_threshold:
				send(msg.sender, msg.receiver, msg.content)
		return True

	def enqueue(self, *messages: model.Message) -> bool:
		for m in messages:
			self.queue.put(m, block=self.block_queue)
		return True

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
			max_queue_size: int = 0,
			default_msg: model.RiskScore = _DEFAULT_MESSAGE,
			local_mode: bool = None):
		super(ShareTraceFactorPart, self).__init__()
		kwargs = {
			'vertex_store': vertex_store,
			'transmission_rate': transmission_rate,
			'time_buffer': time_buffer,
			'time_constant': time_constant,
			'queue': queue,
			'max_queue_size': max_queue_size,
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
			variables: Sequence[ShareTraceVariablePart]) -> bool:
		kwargs = {'graph': graph, 'variables': variables}
		func = self._actor.send_to_variables
		return func(**kwargs) if self.local_mode else func.remote(**kwargs)

	def enqueue(self, *messages: model.Message) -> bool:
		func = self._actor.enqueue
		return func(*messages) if self.local_mode else func.remote(*messages)

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
			max_queue_size: int,
			transmission_rate: float,
			time_buffer: np.timedelta64,
			time_constant: float,
			default_msg: model.RiskScore):
		super(_ShareTraceFactorPart, self).__init__()
		self._check_queue_and_max_queue_size(queue, max_queue_size)
		self._check_transmission_rate(transmission_rate)
		self.vertex_store = vertex_store
		self.block_queue = max_queue_size < 1
		if queue is None:
			self.queue = stores.queue_factory(
				max_size=max_queue_size, local_mode=True)
		else:
			self.queue = queue
		self.transmission_rate = float(transmission_rate)
		self.time_buffer = np.timedelta64(time_buffer, 's')
		self.time_constant = float(time_constant)
		self.default_msg = default_msg

	@staticmethod
	def _check_queue_and_max_queue_size(queue, max_queue_size):
		# TODO(rdt17) Issue warning if both are not None
		if queue is None and max_queue_size is None:
			raise ValueError("Must provide either 'queue' or 'max_queue_size'")

	@staticmethod
	def _check_transmission_rate(value):
		if value < 0 or value > 1:
			raise ValueError(
				"'transmission_rate' must be between 0 and 1, inclusive")

	def send_to_variables(
			self,
			*,
			graph: graphs.FactorGraph,
			variables: Sequence[ShareTraceVariablePart]) -> bool:
		while len(self.queue):
			msg = self.queue.get(block=self.block_queue)
			sender = msg.receiver
			neighbors = tuple(graph.get_neighbors(sender))
			# Assumes factor vertex has a degree of 2
			receiver = neighbors[not neighbors.index(msg.sender)]
			msg = self._compute_message(factor=sender, msg=msg)
			msg = model.Message(sender=sender, receiver=receiver, content=msg)
			v = graph.get_vertex_attr(receiver, key='address')
			variables[v].enqueue(msg)
		return True

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

	def enqueue(self, *messages: model.Message) -> bool:
		for m in messages:
			self.queue.put(m, block=self.block_queue)
		return True

	def kill(self) -> NoReturn:
		pass

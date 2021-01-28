import datetime
import functools
import itertools
import random
from typing import (
	Any, Hashable, Iterable, NoReturn, Optional, Sequence, Tuple, Union)

import codetiming
import numpy as np
import ray

import backend
import graphs
import model
import stores

_TWO_DAYS = np.timedelta64(2, 'D')
_NOW = np.datetime64(backend.TIME, 's')
_DEFAULT_MESSAGE = model.RiskScore(
	name='DEFAULT_ID', timestamp=backend.TIME, value=0)
_KILL_EXCEPTION = '{} does not support kill(); use {} instead.'
RiskScores = Iterable[model.RiskScore]
Maxes = Union[np.ndarray, Iterable[Tuple[graphs.Vertex, model.RiskScore]]]
TimestampBuffer = Union[datetime.timedelta, np.timedelta64]
AllRiskScores = Iterable[Tuple[Hashable, RiskScores]]
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
		'_queue',
		'_graph',
		'_actors',
		'_address_book']

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
			seed: Any = None, ):
		self._check_transmission_rate(transmission_rate)
		self._check_tolerance(tolerance)
		self._check_iterations(iterations)
		self._check_msg_threshold(msg_threshold)
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
		self._queue = stores.queue_factory(
			max_size=max_queue_size,
			local_mode=local_mode)

	@staticmethod
	def _check_transmission_rate(value):
		if value < 0 or value > 1:
			raise ValueError(
				"'transmission_rate' must be between 0 and 1, inclusive")

	@staticmethod
	def _check_tolerance(value):
		if value <= 0:
			raise ValueError("'tolerance' must be greater than 0")

	@staticmethod
	def _check_iterations(value):
		if value < 1:
			raise ValueError("'iterations' must be at least 1")

	@staticmethod
	def _check_msg_threshold(value):
		if not 0 <= value <= 1:
			raise ValueError(
				"'msg_threshold' must be between 0 and 1, inclusive")

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
		result = self._call(factors=factors, variables=variables)
		stdout('------------END BELIEF PROPAGATION------------')
		return result

	@codetiming.Timer(text='Total duration: {:0.6f} s', logger=stdout)
	def _call(self, *, factors: Contacts, variables: AllRiskScores) -> Result:
		self._create_graph(factors=factors, variables=variables)
		maxes = self._get_maxes(only_value=True)
		i, t = 0, np.inf
		while i < self.iterations and t > self.tolerance:
			stdout(f'-----------Iteration {i + 1}-----------')
			remaining = self._send_to_factors()
			self._route_messages(remaining)
			remaining = self._send_to_variables()
			self._route_messages(remaining)
			i, t, maxes = self._update(iteration=i, maxes=maxes)
			stdout(f'---------------------------------')
		maxes = self._get_maxes(with_variable=True)
		self._shutdown()
		return maxes

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Creating graph: {:0.6f} s', logger=stdout)
	def _create_graph(
			self, factors: Contacts, variables: AllRiskScores) -> NoReturn:
		builder = graphs.FactorGraphBuilder(
			impl=self.impl,
			# Graph structure is static; local mode = single-process
			share_graph=not self.local_mode,
			graph_as_actor=False,
			# Separate the stateless (structure) and stateful (attributes)
			use_vertex_store=True,
			# Reserve 1 for queue if in non-local mode
			num_stores=1 if self.local_mode else self._get_num_cpus() - 1,
			store_as_actor=False,
			# Prevent actors from being killed automatically
			detached=True)
		self._add_variables(builder, variables)
		self._add_factors_and_edges(builder, factors)
		graph, factors, variables, vertex_stores = builder.build()
		addresses = itertools.chain(
			itertools.chain.from_iterable(factors),
			itertools.chain.from_iterable(variables))
		self._address_book = {vertex: actor for vertex, actor in addresses}
		self._graph = graph
		self._actors = tuple(
			ShareTraceFGPart(
				factors=tuple(f_id for f_id, _ in f),
				variables=tuple(v_id for v_id, _ in v),
				vertex_store=s,
				max_queue_size=self.max_queue_size,
				local_mode=self.local_mode,
				transmission_rate=self.transmission_rate,
				time_constant=self.time_constant,
				msg_threshold=self.msg_threshold,
				time_buffer=self.time_buffer,
				default_msg=_DEFAULT_MESSAGE,
				current_time=_NOW)
			for f, v, s in zip(factors, variables, vertex_stores))

	@staticmethod
	def _add_variables(
			builder: graphs.FactorGraphBuilder,
			variables: AllRiskScores) -> NoReturn:
		vertices = []
		attrs = {}
		for k, v in ((str(k), v) for k, v in variables):
			vertices.append(k)
			v1, v2 = itertools.tee(v)
			attrs.update({
				k: {
					'max': max(v2, default=_DEFAULT_MESSAGE),
					'inbox': {k: tuple(v1)}}})
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
			attrs.update({k: {'occurrences': f.occurrences, 'inbox': {}}})
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
	@codetiming.Timer(text='Routing messages: {:0.6f} s', logger=stdout)
	def _route_messages(self, refs: Sequence) -> NoReturn:
		if self.local_mode:
			remaining = True
		else:
			_, remaining = ray.wait(refs, timeout=0.001)
		while remaining:
			while len(self._queue):
				msg = self._queue.get(block=bool(self.max_queue_size))
				destination = self._address_book[msg.receiver]
				self._actors[destination].enqueue(msg)
			if self.local_mode:
				remaining = False
			else:
				# Wait for the queue to populate
				_, remaining = ray.wait(remaining, timeout=0.001)

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
			if isinstance(self._queue, backend.ActorMixin):
				self._queue.kill()
			for a in self._actors:
				a.kill()

	def _get_num_cpus(self) -> int:
		return 1 if self.local_mode else backend.NUM_CPUS


class ShareTraceFGPart(graphs.FGPart, backend.ActorMixin):
	__slots__ = ['local_mode', '_actor']

	def __init__(
			self,
			*,
			variables: Sequence[graphs.Vertex],
			factors: Sequence[graphs.Vertex],
			vertex_store: stores.VertexStore,
			max_queue_size: int,
			transmission_rate: float,
			time_buffer: np.timedelta64,
			time_constant: float,
			msg_threshold: float,
			default_msg: model.RiskScore,
			current_time: datetime.datetime,
			local_mode: bool):
		super(ShareTraceFGPart, self).__init__()
		kwargs = {
			'variables': variables,
			'factors': factors,
			'vertex_store': vertex_store,
			'max_queue_size': max_queue_size,
			'transmission_rate': transmission_rate,
			'time_buffer': time_buffer,
			'time_constant': time_constant,
			'msg_threshold': msg_threshold,
			'default_msg': default_msg,
			'current_time': current_time,
			'local_mode': local_mode}
		local_mode = backend.LOCAL_MODE if local_mode is None else local_mode
		self.local_mode = bool(local_mode)
		if local_mode:
			self._actor = _ShareTraceFGPart(**kwargs)
		else:
			self._actor = ray.remote(_ShareTraceFGPart).remote(**kwargs)

	def send_to_factors(
			self,
			*,
			graph: graphs.FactorGraph,
			queue: stores.Queue) -> bool:
		kwargs = {'graph': graph, 'queue': queue}
		send_to_factors = self._actor.send_to_factors
		if self.local_mode:
			result = send_to_factors(**kwargs)
		else:
			result = send_to_factors.remote(**kwargs)
		return result

	def send_to_variables(
			self,
			*,
			graph: graphs.FactorGraph,
			queue: stores.Queue,
			transmission_rate: float,
			buffer: np.datetime64) -> bool:
		kwargs = {'graph': graph, 'queue': queue}
		send_to_variables = self._actor.send_to_variables
		if self.local_mode:
			result = send_to_variables(**kwargs)
		else:
			result = send_to_variables.remote(**kwargs)
		return result

	def get_maxes(
			self,
			*,
			only_value: bool = False,
			with_variable: bool = False) -> Maxes:
		kwargs = {'only_value': only_value, 'with_variable': with_variable}
		get_maxes = self._actor.get_maxes
		if self.local_mode:
			result = get_maxes(**kwargs)
		else:
			result = get_maxes.remote(**kwargs)
		return result

	def enqueue(self, message: model.Message) -> bool:
		enqueue = self._actor.enqueue
		if self.local_mode:
			result = enqueue(message)
		else:
			result = enqueue.remote(message)
		return result

	def kill(self) -> NoReturn:
		if not self.local_mode:
			ray.kill(self._actor)


class _ShareTraceFGPart(graphs.FGPart):
	__slots__ = [
		'variables',
		'factors',
		'vertex_store',
		'queue',
		'block_queue',
		'default_msg',
		'transmission_rate',
		'time_buffer',
		'time_constant',
		'msg_threshold',
		'current_time',
		'local_mode']

	def __init__(
			self,
			*,
			variables: Sequence[graphs.Vertex],
			factors: Sequence[graphs.Vertex],
			vertex_store: stores.VertexStore,
			max_queue_size: int,
			transmission_rate: float,
			time_buffer: np.timedelta64,
			time_constant: float,
			msg_threshold: float,
			default_msg: model.RiskScore,
			current_time: datetime.datetime,
			local_mode: bool):
		super(_ShareTraceFGPart, self).__init__()
		self.variables = np.unique(variables)
		self.factors = np.unique(factors)
		self.vertex_store = vertex_store
		self.block_queue = max_queue_size < 1
		self.queue = stores.queue_factory(
			max_size=max_queue_size, local_mode=True)
		self.transmission_rate = float(transmission_rate)
		self.time_buffer = np.timedelta64(time_buffer, 's')
		self.time_constant = float(time_constant)
		self.msg_threshold = float(msg_threshold)
		self.default_msg = default_msg
		self.current_time = np.datetime64(current_time, 's')
		self.local_mode = bool(local_mode)

	def send_to_factors(
			self,
			*,
			graph: graphs.FactorGraph,
			queue: stores.Queue) -> bool:
		def update_max(vertex, mx, incoming):
			# 'max' is already set when creating graph
			if vertex not in incoming:
				updated = max(itertools.chain(incoming.values(), [mx]))
				updated = {vertex: {'max': updated}}
				self.vertex_store.put(
					keys=[vertex], attributes=updated, merge=False)

		def send(sender, sender_max, incoming):
			for f in graph.get_neighbors(sender):
				if sender in incoming:  # First iteration only
					others = incoming[sender]
				else:
					# Avoid self-bias by excluding the message sent from the
					# receiving factor vertex
					others = (
						msg for o, msg in incoming.items()
						if msg.value >= self.msg_threshold and o != f)
				msg = itertools.chain([sender_max], others)
				if not self.local_mode:
					msg = np.array(list(msg))
				msg = model.Message(sender=sender, receiver=f, content=msg)
				queue.put(msg, block=self.block_queue)

		self._update_inboxes()
		for v in self.variables:
			attributes = self.vertex_store.get(key=v)
			inbox = attributes['inbox']
			curr_max = attributes['max']
			update_max(v, curr_max, inbox)
			send(v, curr_max, inbox)
		self._clear_inboxes(variables=True)
		return True

	def send_to_variables(
			self,
			*,
			graph: graphs.FactorGraph,
			queue: stores.Queue) -> bool:
		self._update_inboxes()
		for f in self.factors:
			neighbors = tuple(graph.get_neighbors(f))
			inbox = self.vertex_store.get(key=f, attribute='inbox')
			for i, v in enumerate(neighbors):
				# Assumes factor vertex has a degree of 2
				content = self._compute_message(
					factor=f, msgs=inbox[neighbors[not i]])
				if content is not None:
					msg = model.Message(sender=f, receiver=v, content=content)
					queue.put(msg, block=self.block_queue)
		self._clear_inboxes(variables=False)
		return True

	def _compute_message(
			self,
			factor: graphs.Vertex,
			msgs: Iterable[model.RiskScore]) -> Optional[model.RiskScore]:
		def sec_to_day(a: np.ndarray) -> np.ndarray:
			return np.array(a, dtype=np.float64, copy=False) / 86400

		occurs = self.vertex_store.get(key=factor, attribute='occurrences')
		occurs = np.array([o.as_array() for o in occurs]).flatten()
		msgs = np.array([m.as_array() for m in msgs]).flatten()
		most_recent = np.max(occurs['timestamp'])
		m = np.where(msgs['timestamp'] <= most_recent + self.time_buffer)
		# Order messages in ascending order
		old_enough = np.sort(msgs[m], order=['timestamp', 'value', 'name'])
		if not old_enough.size:
			msg = self.default_msg
		else:
			# Formats each time delta as partial days
			diff = sec_to_day(old_enough['timestamp'] - most_recent)
			np.clip(diff, -np.inf, 0, out=diff)
			# Newer messages are weighted more with a smaller decay weight
			weight = np.exp(diff / self.time_constant)
			# Newer messages account for the weight of older messages (causal)
			norm = np.cumsum(weight)
			weighted = np.cumsum(old_enough['value'] * weight)
			weighted *= self.transmission_rate / norm
			# Select the message with the maximum weighted average
			ind = np.argmax(weighted)
			old_enough[ind]['value'] = weighted[ind]
			msg = model.RiskScore.from_array(old_enough[ind])
		return msg

	def get_maxes(
			self,
			*,
			only_value: bool = False,
			with_variable: bool = False) -> Maxes:
		get_max = functools.partial(self.vertex_store.get, attribute='max')
		maxes = ((v, get_max(key=v)) for v in self.variables)
		if only_value:
			if with_variable:
				maxes = tuple((v, m.value) for v, m in maxes)
			else:
				maxes = np.array([m.value for _, m in maxes])
		else:
			if with_variable:
				maxes = tuple(maxes)
			else:
				maxes = np.array([m for _, m in maxes])
		return maxes

	def _update_inboxes(self) -> NoReturn:
		while len(self.queue):
			msg = self.queue.get(block=self.block_queue)
			attrs = {msg.receiver: {'inbox': {msg.sender: msg.content}}}
			self.vertex_store.put(
				keys=[msg.receiver], attributes=attrs, merge=True)

	def _clear_inboxes(self, *, variables: bool) -> NoReturn:
		def clear(vertices: np.ndarray):
			attributes = {v: {'inbox': {}} for v in vertices}
			self.vertex_store.put(
				keys=vertices, attributes=attributes, merge=False)

		clear(self.variables) if variables else clear(self.factors)

	def enqueue(self, message: model.Message) -> bool:
		return self.queue.put(message, block=self.block_queue)

import datetime
import functools
import itertools
import random
from typing import Any, Hashable, Iterable, Mapping, NoReturn, Optional, \
	Sequence, Tuple, Union

import attr
import codetiming
import numpy as np
import ray

import backend
import graphs
import model
import stores

_TWO_DAYS = np.timedelta64(datetime.timedelta(days=2), 's')
_NOW = np.datetime64(backend.TIME, 's')
_DEFAULT_MESSAGE = model.RiskScore(
	name='DEFAULT_ID', timestamp=backend.TIME, value=0)
RiskScores = Iterable[model.RiskScore]
AllRiskScores = Iterable[Tuple[Hashable, RiskScores]]
Contacts = Iterable[model.Contact]
Vertices = Union[np.ndarray, ray.ObjectRef]
OptionalObjectRefs = Optional[Sequence[ray.ObjectRef]]
Result = Iterable[Tuple[graphs.Vertex, model.RiskScore]]
stdout = backend.STDOUT
stderr = backend.STDERR


class FactorGraphPartition:
	__slots__ = ['local_mode', '_actor']

	def __init__(
			self,
			*,
			variables: Sequence[graphs.Vertex],
			factors: Sequence[graphs.Vertex],
			vertex_store: stores.VertexStore,
			max_queue_size: int = 0,
			local_mode: bool = True,
			default_msg: model.RiskScore = _DEFAULT_MESSAGE,
			current_time: datetime.datetime = _NOW):
		kwargs = {
			'variables': variables,
			'factors': factors,
			'vertex_store': vertex_store,
			'max_queue_size': max_queue_size,
			'local_mode': local_mode,
			'default_msg': default_msg,
			'current_time': current_time}
		self.local_mode = bool(local_mode)
		if local_mode:
			self._actor = _FactorGraphPartition(**kwargs)
		else:
			self._actor = ray.remote(_FactorGraphPartition).remote(**kwargs)

	def send_to_factors(
			self,
			*,
			graph: graphs.FactorGraph,
			queue: stores.Queue) -> bool:
		kwargs = {'graph': graph, 'queue': queue}
		if self.local_mode:
			result = self._actor.send_to_factors(**kwargs)
		else:
			result = self._actor.send_to_factors.remote(**kwargs)
		return result

	def send_to_variables(
			self,
			*,
			graph: graphs.FactorGraph,
			queue: stores.Queue,
			transmission_rate: float,
			buffer: np.datetime64) -> bool:
		kwargs = {
			'graph': graph,
			'queue': queue,
			'transmission_rate': transmission_rate,
			'buffer': buffer}
		if self.local_mode:
			result = self._actor.send_to_variables(**kwargs)
		else:
			result = self._actor.send_to_variables.remote(**kwargs)
		return result

	def get_maxes(
			self,
			*,
			only_value: bool = False,
			with_variable: bool = False
	) -> Union[np.ndarray, Iterable[Tuple[graphs.Vertex, model.RiskScore]]]:
		kwargs = {'only_value': only_value, 'with_variable': with_variable}
		if self.local_mode:
			result = self._actor.get_maxes(**kwargs)
		else:
			result = self._actor.get_maxes.remote(**kwargs)
		return result

	def get_variables(self) -> np.ndarray:
		if self.local_mode:
			result = self._actor.get_variables()
		else:
			result = self._actor.get_variables.remote()
		return result

	def enqueue(self, message: model.Message) -> NoReturn:
		if self.local_mode:
			self._actor.enqueue(message)
		else:
			self._actor.enqueue.remote(message)

	def kill(self):
		if not self.local_mode:
			ray.kill(self._actor)


class _FactorGraphPartition:
	__slots__ = [
		'_variables',
		'_factors',
		'_vertex_store',
		'_queue',
		'_max_queue_size',
		'_local_mode',
		'_default_msg',
		'_current_time']

	def __init__(
			self,
			*,
			variables: Sequence[graphs.Vertex],
			factors: Sequence[graphs.Vertex],
			vertex_store: stores.VertexStore,
			max_queue_size: int = 0,
			local_mode: bool = True,
			default_msg: model.RiskScore = _DEFAULT_MESSAGE,
			current_time: datetime.datetime = _NOW):
		self._variables = np.unique(variables)
		self._factors = np.unique(factors)
		self._vertex_store = vertex_store
		self._max_queue_size = int(max_queue_size)
		self._queue = stores.Queue(local_mode=True, max_size=max_queue_size)
		self._local_mode = bool(local_mode)
		self._default_msg = default_msg
		self._current_time = np.datetime64(current_time, 's')

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
				self._vertex_store.put(
					keys=[vertex], attributes=updated, merge=False)

		def send(sender, sender_max, incoming):
			for f in graph.get_neighbors(sender):
				if sender in incoming:  # First iteration only
					others = incoming[sender]
				else:
					# Avoid self-bias by excluding the message sent from the
					# receiving factor vertex
					others = (msg for o, msg in incoming.items() if o != f)
				msg = itertools.chain([sender_max], others)
				if not self._local_mode:
					msg = np.array(list(msg))
				msg = model.Message(sender=sender, receiver=f, content=msg)
				queue.put(msg, block=bool(self._max_queue_size))

		self._update_inboxes()
		for v in self._variables:
			attributes = self._vertex_store.get(key=v)
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
			queue: stores.Queue,
			transmission_rate: float,
			buffer: np.datetime64) -> bool:
		self._update_inboxes()
		for f in self._factors:
			neighbors = tuple(graph.get_neighbors(f))
			inbox = self._vertex_store.get(key=f, attribute='inbox')
			for i, v in enumerate(neighbors):
				# Assumes factor vertex has a degree of 2
				content = self._compute_message(
					factor=f,
					msgs=inbox[neighbors[not i]],
					transmission_rate=transmission_rate,
					buffer=buffer)
				if content is not None:
					msg = model.Message(sender=f, receiver=v, content=content)
					queue.put(msg, block=bool(self._max_queue_size))
		self._clear_inboxes(variables=False)
		return True

	def _compute_message(
			self,
			factor: graphs.Vertex,
			msgs: Iterable[model.RiskScore],
			transmission_rate: float,
			buffer: np.datetime64) -> Optional[model.RiskScore]:
		def sec_to_day(a: np.ndarray) -> np.ndarray:
			return np.array(a, dtype=np.float64, copy=False) / 86400

		occurs = self._vertex_store.get(key=factor, attribute='occurrences')
		occurs = np.array([o.as_array() for o in occurs]).flatten()
		msgs = np.array([m.as_array() for m in msgs]).flatten()
		m = np.where(msgs['timestamp'] <= np.max(occurs['timestamp']) + buffer)
		# Order messages in ascending order
		old_enough = np.sort(msgs[m], order=['timestamp', 'value', 'name'])
		if not old_enough.size:
			msg = self._default_msg
		else:
			# Formats each time delta as partial days
			# TODO Should this be current time or msg timestamp?
			diff = sec_to_day(old_enough['timestamp'] - self._current_time)
			# TODO Only necessary if using msg timestamp; current time
			#  ensures all diffs are less than 0
			np.clip(diff, -np.inf, 0, out=diff)
			# Newer messages are weighted more with a smaller decay weight
			weight = np.exp(diff)
			# Newer messages account for the weight of older messages (causal)
			norm = np.cumsum(weight)
			weighted = np.cumsum(old_enough['value'] * weight)
			weighted *= transmission_rate / norm
			# Select the message with the maximum weighted average
			ind = np.argmax(weighted)
			old_enough[ind]['value'] = weighted[ind]
			msg = model.RiskScore.from_array(old_enough[ind])
		return msg

	def get_maxes(
			self,
			*,
			only_value: bool = False,
			with_variable: bool = False
	) -> Union[np.ndarray, Iterable[Tuple[graphs.Vertex, model.RiskScore]]]:
		get_max = functools.partial(self._vertex_store.get, attribute='max')
		maxes = ((v, get_max(key=v)) for v in self._variables)
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
		while self._queue.qsize():
			msg = self._queue.get(block=bool(self._max_queue_size))
			attrs = {msg.receiver: {'inbox': {msg.sender: msg.content}}}
			self._vertex_store.put(
				keys=[msg.receiver], attributes=attrs, merge=True)

	def _clear_inboxes(self, *, variables: bool) -> NoReturn:
		def clear(vertices: np.ndarray):
			attributes = {v: {'inbox': {}} for v in vertices}
			self._vertex_store.put(
				keys=vertices, attributes=attributes, merge=False)

		clear(self._variables) if variables else clear(self._factors)

	def get_variables(self) -> np.ndarray:
		return self._variables

	def enqueue(self, message) -> NoReturn:
		self._queue.put(message, block=bool(self._max_queue_size))


@attr.s(slots=True)
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

		timestamp_buffer: The amount of time (in seconds) that must have
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
	transmission_rate = attr.ib(type=float, default=0.8, converter=float)
	tolerance = attr.ib(type=float, default=1e-10, converter=float)
	iterations = attr.ib(type=int, default=4, converter=int)
	timestamp_buffer = attr.ib(
		type=Union[datetime.timedelta, np.timedelta64],
		default=_TWO_DAYS,
		converter=np.timedelta64)
	max_queue_size = attr.ib(type=int, default=0, converter=int)
	impl = attr.ib(type=str, default=graphs.DEFAULT)
	seed = attr.ib(type=Any, default=None)
	local_mode = attr.ib(type=bool, default=None)
	_queue = attr.ib(type=stores.Queue, init=False, repr=False)
	_graph = attr.ib(type=graphs.FactorGraph, init=False, repr=False)
	_actors = attr.ib(
		type=Sequence[FactorGraphPartition], init=False, repr=False)
	_address_book = attr.ib(
		type=Mapping[Hashable, Hashable], init=False, repr=False)

	def __attrs_post_init__(self):
		if self.local_mode is None:
			self.local_mode = backend.LOCAL_MODE
		self._queue = stores.Queue(
			local_mode=self.local_mode,
			max_size=self.max_queue_size,
			detached=True)
		if self.seed is not None:
			random.seed(self.seed)
			np.random.seed(self.seed)

	@transmission_rate.validator
	def _check_transmission_rate(self, attribute, value):
		if value < 0 or value > 1:
			raise ValueError(
				"'transmission_rate' must be between 0 and 1, inclusive")

	@tolerance.validator
	def _check_tolerance(self, attribute, value):
		if value <= 0:
			raise ValueError("'tolerance' must be greater than 0")

	@iterations.validator
	def _check_iterations(self, attribute, value):
		if value < 1:
			raise ValueError("'iterations' must be at least 1")

	@impl.validator
	def _check_backend(self, attribute, value):
		if value not in graphs.OPTIONS:
			raise ValueError(f"'impl' must be one of {graphs.OPTIONS}")

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
			FactorGraphPartition(
				factors=tuple(f_id for f_id, _ in f),
				variables=tuple(v_id for v_id, _ in v),
				vertex_store=s,
				max_queue_size=self.max_queue_size,
				local_mode=self.local_mode,
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
		builder.add_variables(vertices, attrs)

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
		builder.add_factors(vertices, attrs)
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
			while self._queue.qsize():
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
			'buffer': self.timestamp_buffer}
		return [a.send_to_variables(**kwargs) for a in self._actors]

	def _update(
			self,
			iteration: int,
			maxes: np.ndarray) -> Tuple[int, float, np.ndarray]:
		iter_maxes = [a.get_maxes(only_value=True) for a in self._actors]
		iter_maxes = self._collect_results(iter_maxes)
		tolerance = np.float64(np.sum(iter_maxes - maxes))
		stdout(f'Tolerance: {np.round(tolerance, 10)}')
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
			self._queue.kill()
			for a in self._actors:
				a.kill()
			if isinstance(self._graph, graphs.FactorGraph):
				self._graph.kill()

	def _get_num_cpus(self) -> int:
		return 1 if self.local_mode else backend.NUM_CPUS

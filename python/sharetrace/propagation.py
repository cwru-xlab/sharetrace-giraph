import datetime
import functools
import itertools
import random
from typing import Any, Hashable, Iterable, NoReturn, Optional, Sequence, \
	Sized, Tuple, Union

import attr
import codetiming
import numpy as np
import ray

import backend
import graphs
import model
import stores

_TWO_DAYS = np.timedelta64(datetime.timedelta(days=2))
_NOW = np.datetime64(backend.TIME, 's')
_DEFAULT_MESSAGE = model.RiskScore(
	name='DEFAULT_ID', timestamp=backend.TIME, value=0)
_REF_GRAPH_OBJECT_MSG = (
	'Factor graph must be a FactorGraph instances to run in local mode')
_NON_REF_GRAPH_OBJECT_MSG = (
	'Factor graph must be an ObjectRef instance to run in non-local mode')
RiskScores = Iterable[model.RiskScore]
AllRiskScores = Iterable[Tuple[Hashable, RiskScores]]
Contacts = Iterable[model.Contact]
Vertices = Union[np.ndarray, ray.ObjectRef]
OptionalObjectRefs = Optional[Sequence[ray.ObjectRef]]
Result = Iterable[Tuple[graphs.Vertex, model.RiskScore]]
stdout = backend.STDOUT
stderr = backend.STDERR


@attr.s(slots=True)
class BeliefPropagation:
	"""A factor graph that performs a variation of belief propagation to
	compute the propagated risk of exposure to a condition among a
	population. Factor vertices represent contacts between pairs of people,
	with vertex data containing all occurrences (time-duration pairs) in the
	recent past that two individuals came into sufficiently long contact.
	Variable vertices represent individuals, with vertex data containing the
	local risk scores, the maximum of these scores, and all risk scores sent
	from neighboring factor vertices.

	Following the core message-passing principle of belief propagation,
	the algorithm performs iterative computation between the factor and
	variable vertex sets. The algorithm begins with all variable vertices
	selecting their maximum local score and sending it to all of their
	neighboring factor vertices. Once this is done, all factor vertices filter
	the risk scores, based on when the individuals came into contact. A
	weighted transformation that accounts for the amount of time passed from
	when the risk score was recorded and the time of running the algorithm is
	applied to all risk scores from a variable vertex. The maximum of these
	is sent to the other variable vertex connected to the factor vertex.
	This completes one iteration of the algorithm and is repeated until
	either a certain number of iterations has passed or the summed difference
	in variable risk scores from the previous iteration drops below a set
	tolerance, whichever condition is satisfied first.
	"""
	transmission_rate = attr.ib(type=float, default=1, converter=float)
	tolerance = attr.ib(type=float, default=1e-10, converter=float)
	iterations = attr.ib(type=int, default=4, converter=int)
	timestamp_buffer = attr.ib(
		type=datetime.datetime, default=_TWO_DAYS, converter=np.timedelta64)
	queue_max_size = attr.ib(type=Optional[int], default=None)
	backend = attr.ib(type=str, default=graphs.DEFAULT)
	seed = attr.ib(type=Any, default=None)
	local_mode = attr.ib(type=bool, default=None)
	_queue = attr.ib(type=stores.Queue, init=False, repr=False)
	_graph = attr.ib(type=graphs.FactorGraph, init=False, repr=False)
	_vertex_store = attr.ib(type=stores.VertexStore, init=False, repr=False)
	_factors = attr.ib(type=Iterable[graphs.Vertex], init=False, repr=False)
	_variables = attr.ib(type=Iterable[graphs.Vertex], init=False, repr=False)
	_num_factors = attr.ib(type=int, init=False, repr=False)
	_num_variables = attr.ib(type=int, init=False, repr=False)

	def __attrs_post_init__(self):
		if self.local_mode is None:
			self.local_mode = backend.LOCAL_MODE
		self._queue = stores.Queue(
			local_mode=self.local_mode,
			max_size=self.queue_max_size,
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

	@queue_max_size.validator
	def _check_max_size(self, attribute, value):
		if value is not None:
			if not isinstance(value, int):
				raise TypeError("'queue_max_size' must of type int or None")
			if value < 0:
				raise ValueError("'queue_max_size' must be least 1")

	@backend.validator
	def _check_backend(self, attribute, value):
		if value not in graphs.OPTIONS:
			raise ValueError(f"'backend' must be one of {graphs.OPTIONS}")

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
			self._send_to_factors()
			self._send_to_variables()
			i, t, maxes = self._update(iteration=i, maxes=maxes)
			stdout(f'---------------------------------')
		variables = self._get_variables(as_ref=False)
		maxes = self._get_maxes()
		self._shutdown()
		return zip(variables, maxes)

	@codetiming.Timer(text='Creating graph: {:0.6f} s', logger=stdout)
	def _create_graph(
			self,
			factors: Contacts,
			variables: AllRiskScores) -> NoReturn:
		builder = graphs.FactorGraphBuilder(
			# Local mode = single-process
			# True results in ObjectLossError when in non-local mode
			as_actor=False,
			backend=self.backend,
			# Graph structure is static; local mode = single-process
			share_graph=not self.local_mode,
			graph_as_actor=False,
			# Separate the stateless (structure) and stateful (attributes)
			use_vertex_store=True,
			# Local mode = single-process
			vertex_store_as_actor=not self.local_mode,
			# Prevents actors from being killed automatically
			detached=not self.local_mode)
		# Must be non-remote since builder is non-remote
		self._add_variables(builder, variables)
		self._add_factors_and_edges(builder, factors)
		if self.local_mode:
			graph, vertex_store = builder.build()
			self._factors = np.array(list(graph.get_factors()))
			self._variables = np.array(list(graph.get_variables()))
			graph.set_factors([])
			graph.set_variables([])
		else:
			graph, factors, variables, vertex_store = builder.build()
			self._factors = factors
			self._variables = variables
		builder.kill()
		self._check_graph_type(graph)
		self._graph = graph
		self._vertex_store = vertex_store
		self._num_factors = len(self._get_factors(as_ref=False))
		self._num_variables = len(self._get_variables(as_ref=False))

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
				k: {'local': frozenset(v1), 'max': max(v2), 'inbox': {}}})
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

	def _check_graph_type(self, graph):
		if self.local_mode and not isinstance(graph, graphs.FactorGraph):
			raise TypeError(_REF_GRAPH_OBJECT_MSG)
		if not self.local_mode and not isinstance(graph, ray.ObjectRef):
			raise TypeError(_NON_REF_GRAPH_OBJECT_MSG)

	@codetiming.Timer(text='Sending to factors: {:0.6f} s', logger=stdout)
	def _send_to_factors(self) -> NoReturn:
		remaining = self._send_to(variables=False)
		self._update_inboxes(variables=False, remaining=remaining)

	@codetiming.Timer(text='Sending to variables: {:0.6f} s', logger=stdout)
	def _send_to_variables(self) -> NoReturn:
		remaining = self._send_to(variables=True)
		self._update_inboxes(variables=True, remaining=remaining)

	def _send_to(self, *, variables: bool) -> OptionalObjectRefs:
		kwargs = {
			'graph': self._graph,
			'vertex_store': self._vertex_store,
			'msg_queue': self._queue,
			'block_queue': self._block_queue(),
			'transmission_rate': self.transmission_rate,
			'buffer': self.timestamp_buffer,
			'local_mode': self.local_mode}
		num_vertices = self._num_factors if variables else self._num_variables
		num_cpus = self._get_num_cpus(variables=not variables)
		ranges = self._get_ranges(vertices=num_vertices, num_cpus=num_cpus)
		to_vertices = self._to_variables if variables else self._to_factors
		vertices = self._get_vertices(variables=not variables, as_ref=True)
		if self.local_mode:
			to_vertices(vertices=vertices, indices=ranges[0], **kwargs)
		else:
			return [
				ray.remote(to_vertices).remote(
					vertices=vertices, indices=indices, **kwargs)
				for indices in ranges]

	@staticmethod
	def _to_factors(
			*,
			graph: graphs.FactorGraph,
			vertex_store: stores.VertexStore,
			vertices: np.ndarray,
			msg_queue: stores.Queue,
			block_queue: bool,
			indices: np.ndarray,
			local_mode: bool,
			**kwargs):
		for v in vertices[indices]:
			attributes = vertex_store.get(key=v)
			inbox = attributes['inbox']
			local = attributes['local']
			for f in graph.get_neighbors(v):
				from_others = (msg for o, msg in inbox.items() if o != f)
				content = itertools.chain(local, from_others)
				if not local_mode:
					content = np.array(list(content))
				msg = model.Message(sender=v, receiver=f, content=content)
				msg_queue.put(msg, block=block_queue)

	@staticmethod
	def _to_variables(
			*,
			graph: graphs.FactorGraph,
			vertices: np.ndarray,
			vertex_store: stores.VertexStore,
			msg_queue: stores.Queue,
			block_queue: bool,
			indices: np.ndarray,
			transmission_rate: float,
			buffer: np.datetime64,
			**kwargs) -> NoReturn:
		for f in vertices[indices]:
			neighbors = tuple(graph.get_neighbors(f))
			for i, v in enumerate(neighbors):
				# Assumes factor vertex has a degree of 2
				neighbor = neighbors[not i]
				attributes = vertex_store.get(key=neighbor)
				local = attributes['local']
				inbox = attributes['inbox'].values()
				content = BeliefPropagation._compute_message(
					vertex_store=vertex_store,
					factor=f,
					messages=itertools.chain(local, inbox),
					transmission_rate=transmission_rate,
					buffer=buffer)
				msg = model.Message(sender=f, receiver=v, content=content)
				msg_queue.put(msg, block=block_queue)

	@staticmethod
	def _compute_message(
			vertex_store: stores.VertexStore,
			factor: graphs.Vertex,
			messages: Iterable[model.RiskScore],
			transmission_rate: float,
			buffer: np.datetime64) -> model.RiskScore:
		"""Computes the message to send from a factor to a variable.

		Only messages that occurred sufficiently before at least one factor
		value are considered. Messages undergo a weighted transformation,
		based on the amount of time between the message's timestamp and the
		most recent message's timestamp and transmission rate. If no
		messages satisfy the initial condition, a defaults message is sent.
		Otherwise, the maximum weighted message is sent.
		"""

		def sec_to_day(a: np.ndarray):
			return np.float64(a) / 86400

		occurs = vertex_store.get(key=factor, attribute='occurrences')
		occurs = np.array([o.as_array() for o in occurs]).flatten()
		messages = np.array([m.as_array() for m in messages]).flatten()
		m = np.where(
			messages['timestamp'] <= np.max(occurs['timestamp']) - buffer)
		# Order messages in ascending order
		old_enough = np.sort(messages[m], order=['timestamp', 'value', 'name'])
		if not len(old_enough):
			msg = _DEFAULT_MESSAGE
		else:
			diff = old_enough['timestamp'] - _NOW
			diff = sec_to_day(np.array(diff, dtype='timedelta64[h]'))
			# Newer messages are weighted more with a smaller decay weight
			weight = np.exp(diff)
			# Newer messages account for the weight of older messages
			norm = np.cumsum(weight)
			weighted = np.cumsum(old_enough['value'] * weight)
			weighted *= transmission_rate / norm
			# Select the message with the maximum weighted average
			ind = np.argmax(weighted)
			old_enough[ind]['value'] = weighted[ind]
			msg = model.RiskScore.from_array(old_enough[ind])
		return msg

	@staticmethod
	def _get_ranges(
			*,
			vertices: Union[Sized, int],
			num_cpus: int = None) -> Sequence[np.ndarray]:
		if isinstance(vertices, Sized):
			num_vertices = len(vertices)
		else:
			num_vertices = int(vertices)
		num_cpus = num_cpus if num_cpus else backend.NUM_CPUS
		step = int(np.ceil(num_vertices / num_cpus))
		return tuple(
			np.arange(n * step, min(num_vertices, step * (n + 1)) - 1)
			for n in range(num_cpus))

	def _get_maxes(
			self,
			only_value: bool = False) -> Union[RiskScores, Iterable[float]]:
		get_max = functools.partial(self._vertex_store.get, attribute='max')
		maxes = (get_max(key=v) for v in self._get_variables(as_ref=False))
		if only_value:
			maxes = np.array([m.value for m in maxes])
		else:
			maxes = np.array(list(maxes))
		return maxes

	@codetiming.Timer(text='Updating: {:0.6f} s', logger=stdout)
	def _update(
			self,
			iteration: int,
			maxes: np.ndarray) -> Tuple[int, float, np.ndarray]:
		iter_maxes = self._update_maxes()
		tolerance = np.float64(np.sum(iter_maxes - maxes))
		maxes = iter_maxes
		self._clear_inboxes()
		iteration += 1
		stdout(f'Tolerance: {np.round(tolerance, 10)}')
		return iteration, tolerance, maxes

	def _update_maxes(self) -> np.ndarray:
		updated = []
		for v in self._get_variables(as_ref=False):
			attributes = self._vertex_store.get(key=v)
			inbox = attributes['inbox'].values()
			mx = attributes['max']
			mx = max(itertools.chain(inbox, [mx]))
			attributes['max'] = mx
			self._vertex_store.put(keys=[v], attributes={v: attributes})
			updated.append(mx.value)
		return np.array(updated)

	def _update_inboxes(
			self, *,
			remaining: OptionalObjectRefs,
			variables: bool) -> NoReturn:
		kwargs = {
			'msg_queue': self._queue,
			'block_queue': self._block_queue(),
			'vertex_store': self._vertex_store,
			'local_mode': self.local_mode}
		if self.local_mode:
			self._inboxes(remaining=None, **kwargs)
		else:
			inboxes = ray.remote(self._inboxes)
			num_cpus = self._get_num_cpus(variables=variables)
			remaining = np.array_split(remaining, num_cpus)
			ray.get([inboxes.remote(remaining=r, **kwargs) for r in remaining])

	@staticmethod
	def _inboxes(
			*,
			remaining: OptionalObjectRefs,
			msg_queue: stores.Queue,
			block_queue: bool,
			vertex_store: stores.VertexStore,
			local_mode: bool) -> NoReturn:
		remaining = True if local_mode else list(remaining)
		while remaining:
			while len(msg_queue):
				msg = msg_queue.get(block=block_queue)
				attrs = {msg.receiver: {'inbox': {msg.sender: msg.content}}}
				vertex_store.put(keys=[msg.receiver], attributes=attrs)
			if local_mode:
				remaining = False
			else:
				_, remaining = ray.wait(remaining, timeout=0.01)

	def _clear_inboxes(self) -> NoReturn:
		def clear(vertices: np.ndarray):
			attributes = dict.fromkeys(vertices, {'inbox': {}})
			self._vertex_store.put(keys=vertices, attributes=attributes)

		clear(self._get_variables(as_ref=False))
		clear(self._get_factors(as_ref=False))

	def _get_variables(self, *, as_ref: bool = True) -> Vertices:
		return self._get_vertices(variables=True, as_ref=as_ref)

	def _get_factors(self, *, as_ref: bool = True) -> Vertices:
		return self._get_vertices(variables=False, as_ref=as_ref)

	def _get_vertices(self, *, variables: bool, as_ref: bool) -> Vertices:
		vertices = self._variables if variables else self._factors
		return vertices if self.local_mode or as_ref else ray.get(vertices)

	def _get_num_cpus(self, *, variables: bool) -> int:
		if self.local_mode:
			num_cpus = 1
		else:
			num_cpus = backend.NUM_CPUS / 2 if variables else backend.NUM_CPUS
		return int(num_cpus)

	def _block_queue(self) -> bool:
		# Blocking in local mode exposes missing await exception
		return bool(self.queue_max_size)

	def _shutdown(self) -> NoReturn:
		if not self.local_mode:
			self._vertex_store.kill()
			self._queue.kill()
			if isinstance(self._graph, graphs.FactorGraph):
				self._graph.kill()

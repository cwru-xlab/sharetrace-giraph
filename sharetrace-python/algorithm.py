import datetime
import functools
import itertools
import random
from typing import Any, Hashable, Iterable, NoReturn, Optional, Sequence, \
	Sized, Tuple, Union

import codetiming
import numpy as np
import ray
from ray.util import queue

import backend
import graphs
import model

"""
Optimizations:
	- Use itertools over custom implementations
	- Use generators for intermediate results
	- Use numpy array over standard list or tuple
	- Use numpy structured arrays over standard Python objects
	- Only make copies of mutable collections if modifying them
"""

# TODO Update all docstrings

_2_DAYS = np.timedelta64(datetime.timedelta(days=2))
_NOW = np.datetime64(datetime.datetime.utcnow(), 's')
_DEFAULT_MESSAGE = model.RiskScore(
	name='DEFAULT_ID', timestamp=datetime.datetime.utcnow(), value=0)
_RiskScores = Iterable[model.RiskScore]
_GroupedRiskScores = Iterable[Tuple[Hashable, _RiskScores]]
_Contacts = Iterable[model.Contact]
log = backend.LOGGER


class BeliefPropagation:
	"""A factor graph that performs a variation of belief propagation to
	compute the propagated risk of exposure to a condition among a
	population. Factor vertices represent contacts between pairs of people,
	with vertex data containing all occurrences (time-duration pairs) in the
	recent past that two individuals came into sufficiently long contact.
	Variable vertices represent individuals, with vertex data containing the
	max risk score as well as all risk scores sent from neighboring factor
	vertices.

	Following the core msg-passing principle of belief propagation,
	the algorithm performs iterative computation between the factor and
	variable vertex sets. The algorithm begins with all variable vertices
	selecting their maximum local score and sending it to all of their
	neighboring factor vertices. Once this is done, all factor vertices filter
	the risk scores, based on when the individuals came into contact,
	and relays all risk scores sent from one individual to the other individual
	involved in the contact. This completes one iteration of the algorithm
	and is repeated until either a certain number of iterations has passed or
	the summed difference in variable risk scores from the previous iteration
	drops below a set tolerance, whichever condition is satisfied first.

	Variable vertex name:
		- HAT name

	Variable vertex values:
		- local risk scores
		- max risk score
		- risk scores received from neighboring factor vertices

	Factor vertex name:
		- HAT names of the contact

	Factor vertex values:
		- occurrences (time-duration pairs)

	Edge data:
		- msg between factor and variable vertices
	"""
	__slots__ = [
		'transmission_rate',
		'tolerance',
		'iterations',
		'max_messages',
		'backend',
		'_local_mode',
		'seed',
		'_graph',
		'_factors',
		'_variables',
		'_vertex_store',
		'_queue']

	def __init__(
			self,
			transmission_rate: float = 0.8,
			tolerance: float = 1e-5,
			iterations: int = 4,
			max_messages: Optional[int] = None,
			backend: str = graphs.DEFAULT,
			local_mode: bool = False,
			seed: Any = None):
		self.transmission_rate = self._check_transmission_rate(
			transmission_rate)
		self.tolerance = self._check_tolerance(tolerance)
		self.iterations = self._check_iterations(iterations)
		self.max_messages = self._check_max_messages(max_messages)
		if self.max_messages is None:
			self._queue = queue.Queue()
		else:
			self._queue = queue.Queue(maxsize=self.max_messages)
		self.backend = backend
		self._local_mode = bool(local_mode)
		self.seed = seed
		if seed is not None:
			random.seed(seed)
			np.random.seed(seed)

	@staticmethod
	def _check_transmission_rate(value):
		if value < 0 or value > 1:
			raise ValueError(
				"'transmission_rate' must be between 0 and 1, inclusive")
		return float(value)

	@staticmethod
	def _check_tolerance(value):
		if value <= 0:
			raise ValueError("'tolerance' must be greater than 0")
		return float(value)

	@staticmethod
	def _check_iterations(value):
		if value < 1:
			raise ValueError("'iterations' must be at least 1")
		return int(value)

	@staticmethod
	def _check_max_messages(value):
		if value is not None:
			if not isinstance(value, int):
				raise TypeError("'max_messages' must of type int or None")
			if value < 0:
				raise ValueError("'max_messages' must be least 1")
		return None if value is None else int(value)

	def __call__(
			self,
			factors: _Contacts,
			variables: _GroupedRiskScores
	) -> Iterable[Tuple[graphs.Vertex, model.RiskScore]]:
		with codetiming.Timer(
				text='BELIEF PROPAGATION: {:0.6f} s', logger=log):
			log('-----------Start of algorithm-----------')
			with codetiming.Timer(text='Creating the graph: {:0.6f} s'):
				self._create_graph(factors=factors, variables=variables)
			maxes = self._get_maxes(only_value=True)
			i, t = 0, np.inf
			while i < self.iterations and t > self.tolerance:
				log(f'-----------Iteration {i + 1}-----------')
				with codetiming.Timer(text='To factor: {:0.6f} s', logger=log):
					remaining = self._send_to_factors()
					self._update_inboxes(remaining)
				with codetiming.Timer(
						text='To variable: {:0.6f} s', logger=log):
					remaining = self._send_to_variables()
					self._update_inboxes(remaining)
				with codetiming.Timer(text='Updating: {:0.6f} s', logger=log):
					iter_maxes = self._update_maxes()
					t = np.sum(iter_maxes - maxes)
					maxes = iter_maxes
					self._clear_inboxes()
					i += 1
				log(f'Tolerance: {np.round(t, 6)}')
			log('-----------End of algorithm-----------')
			variables = self._get_variables()
			maxes = self._get_maxes()
			return zip(variables, maxes)

	def _create_graph(
			self,
			factors: _Contacts,
			variables: _GroupedRiskScores) -> NoReturn:
		if self._local_mode:
			builder = graphs.FactorGraphBuilder(
				as_actor=False,
				backend=self.backend,
				share_graph=False,
				graph_as_actor=False,
				use_vertex_store=True,  # Only vertex ids, no attributes
				vertex_store_as_actor=False)
			self._add_variables(builder, variables)
			self._add_factors_and_edges(builder, factors)
			graph, vertex_store = builder.build()
			self._vertex_store = vertex_store
			self._factors = np.array(list(graph.get_factors()))
			self._variables = np.array(list(graph.get_variables()))
			graph.set_factors([])
			graph.set_variables([])
			self._graph = graph
		else:
			builder = graphs.FactorGraphBuilder(
				as_actor=True,  # Must be True to use remote functions
				backend=self.backend,
				share_graph=True,  # Graph structure is static
				graph_as_actor=False,  # Would incur unnecessary overhead
				use_vertex_store=True,
				vertex_store_as_actor=True)  # For later remote functions
			add_variables = ray.remote(self._add_variables)
			ray.get(add_variables.remote(builder, variables))
			add_factors_and_edges = ray.remote(self._add_factors_and_edges)
			ray.get(add_factors_and_edges).remote(builder, factors)
			graph, factors, variables, vertex_store = builder.build()
			self._graph = graph
			self._vertex_store = vertex_store
			self._factors = factors
			self._variables = variables

	@staticmethod
	def _add_variables(
			builder: graphs.FactorGraphBuilder,
			variables: _GroupedRiskScores) -> NoReturn:
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
			factors: _Contacts) -> NoReturn:
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
			only_value: bool = False) -> Union[_RiskScores, Iterable[float]]:
		get_max = functools.partial(self._vertex_store.get, attribute='max')
		maxes = (get_max(key=v) for v in self._get_variables())
		if only_value:
			maxes = np.array([m.value for m in maxes])
		else:
			maxes = np.array(list(maxes))
		return maxes

	def _send_to_factors(self) -> Optional[Sequence[ray.ObjectRef]]:
		if self._local_mode:
			self._to_factors(
				graph=self._graph,
				vertex_store=self._vertex_store,
				variables=self._variables,
				msg_queue=self._queue,
				block_queue=self.max_messages is None,
				indices=np.arange(len(self._variables)))
		else:
			ranges = _get_index_ranges(ray.get(self._variables))
			return [
				ray.remote(self._to_factors).remote(
					graph=self._graph,
					vertex_store=self._vertex_store,
					variables=self._variables,
					msg_queue=self._queue,
					block_queue=self.max_messages is None,
					indices=indices)
				for indices in ranges]

	@staticmethod
	def _to_factors(
			graph: graphs.FactorGraph,
			vertex_store: graphs.VertexStore,
			variables: np.ndarray,
			msg_queue: queue.Queue,
			block_queue: bool,
			indices: np.ndarray):
		for v in variables[indices]:
			inbox = vertex_store.get(key=v, attribute='inbox')
			local = vertex_store.get(key=v, attribute='local')
			for f in graph.get_neighbors(v):
				from_others = {o: msg for o, msg in inbox.items() if o != f}
				content = itertools.chain(local, from_others.values())
				msg = graphs.Message(sender=v, receiver=f, content=content)
				msg_queue.put(msg, block=block_queue)

	def _update_inboxes(
			self,
			remaining: Optional[Sequence[ray.ObjectRef]] = None) -> NoReturn:
		remaining = [] if remaining is None else remaining
		while len(remaining) or self._queue.size():
			if not self._local_mode:
				_, remaining = ray.wait(remaining)
			msg = self._queue.get()
			attributes = self._vertex_store.get(key=msg.receiver)
			attributes['inbox'][msg.sender] = msg.content
			self._vertex_store.put(
				keys=[msg.receiver], attributes={msg.receiver: attributes})

	def _send_to_variables(self) -> Optional[Sequence[ray.ObjectRef]]:
		if self._local_mode:
			self._to_variables(
				graph=self._graph,
				vertex_store=self._vertex_store,
				factors=self._factors,
				msg_queue=self._queue,
				block_queue=self.max_messages is None,
				indices=np.arange(len(self._factors)),
				transmission_rate=self.transmission_rate)
		else:
			ranges = _get_index_ranges(ray.get(self._factors))
			return [
				ray.remote(self._to_variables).remote(
					graph=self._graph,
					vertex_store=self._vertex_store,
					factors=self._factors,
					msg_queue=self._queue,
					block_queue=self.max_messages is None,
					indices=indices,
					transmission_rate=self.transmission_rate)
				for indices in ranges]

	@staticmethod
	def _compute_message(
			vertex_store: graphs.VertexStore,
			factor: graphs.Vertex,
			messages: Iterable[model.RiskScore],
			transmission_rate: float) -> model.RiskScore:
		"""Computes the message to send from a factor to a variable.

		Only messages that occurred sufficiently before at least one factor
		value are considered. Messages undergo a weighted transformation,
		based on the amount of time between the message's timestamp and the
		most recent message's timestamp and transmission rate. If no
		messages satisfy the initial condition, a defaults message is sent.
		Otherwise, the maximum weighted message is sent.

		Args:
			factor: Factor vertex sending the messages.
			messages: Messages in consideration.

		Returns:
			Message to send.
		"""
		occurs = vertex_store.get(key=factor, attribute='occurrences')
		occurs = np.array([o.as_array() for o in occurs]).flatten()
		messages = np.array([m.as_array() for m in messages]).flatten()
		m = np.where(
			messages['timestamp'] <= np.max(occurs['timestamp']) - _2_DAYS)
		# Order messages in ascending order
		old_enough = np.sort(messages[m], order=['timestamp', 'value', 'name'])
		if not len(old_enough):
			msg = _DEFAULT_MESSAGE
		else:
			diff = old_enough['timestamp'] - _NOW
			diff = np.array(diff, dtype='timedelta64[D]')
			# Newer messages are weighted more with a smaller decay weight
			weight = np.exp(np.int16(diff))
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
	def _to_variables(
			graph: graphs.FactorGraph,
			factors: np.ndarray,
			vertex_store: graphs.VertexStore,
			msg_queue: queue.Queue,
			block_queue: bool,
			indices: np.ndarray,
			transmission_rate: float):
		for f in factors[indices]:
			neighbors = tuple(graph.get_neighbors(f))
			for i, v in enumerate(graph.get_neighbors(f)):
				# Assumes factor vertex has a degree of 2
				neighbor = neighbors[not i]
				local = vertex_store.get(key=neighbor, attribute='local')
				inbox = vertex_store.get(key=neighbor, attribute='inbox')
				messages = itertools.chain(local, inbox.values())
				content = BeliefPropagation._compute_message(
					vertex_store=vertex_store,
					factor=f,
					messages=messages,
					transmission_rate=transmission_rate)
				msg = graphs.Message(sender=f, receiver=v, content=content)
				msg_queue.put(msg, block=block_queue)

	def _update_maxes(self) -> np.ndarray:
		updated = []
		for v in self._get_variables():
			attributes = self._vertex_store.get(key=v)
			inbox = attributes['inbox'].values()
			mx = attributes['max']
			mx = max(itertools.chain(inbox, [mx]))
			attributes['max'] = mx
			self._vertex_store.put(keys=[v], attributes={v: attributes})
			updated.append(mx.value)
		return np.array(updated)

	def _clear_inboxes(self) -> NoReturn:
		def clear(vertices: np.ndarray):
			for v in vertices:
				attributes = self._vertex_store.get(key=v)
				attributes['inbox'] = {}
				self._vertex_store.put(keys=[v], attributes={v: attributes})

		clear(self._get_variables())
		clear(self._get_factors())

	def _get_variables(self) -> np.ndarray:
		return self._get_vertices(variables=True)

	def _get_factors(self) -> np.ndarray:
		return self._get_vertices(variables=False)

	def _get_vertices(self, variables: bool = True) -> np.ndarray:
		vertices = self._variables if variables else self._factors
		return vertices if self._local_mode else ray.get(vertices)


def _get_index_ranges(vertices: Sized) -> Iterable[np.ndarray]:
	num_vertices = len(vertices)
	num_cpus = backend.NUM_CPUS
	step = np.ceil(num_vertices / num_cpus)
	stop = functools.partial(min, num_vertices)
	return [
		np.arange(i * step, stop(step * (i + 1)) - 1, dtype=np.int64)
		for i in range(num_cpus)]

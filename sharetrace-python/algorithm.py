import datetime
import functools
import itertools
import random
from typing import Hashable, Iterable, NoReturn, Optional, Sequence, Sized, \
	Tuple

import attr
import codetiming
import numpy as np
import ray
from ray.util import iter as it, queue

import backend
import graphs
import model
from graphs import FactorGraph, Vertex

"""
Optimizations:
	- Use itertools over custom implementations
	- Use generators for intermediate results
	- Use numpy array over standard list or tuple
	- Use numpy structured arrays over standard Python objects
	- Only make copies of mutable collections if modifying them
"""

# TODO Update all docstrings

_TWO_DAYS = np.timedelta64(datetime.timedelta(days=2))
_NOW = np.datetime64(datetime.datetime.utcnow())
_DEFAULT_MESSAGE = model.RiskScore(
	id='DEFAULT_ID', timestamp=datetime.datetime.utcnow(), value=0).as_array()
_RiskScores = Iterable[model.RiskScore]
_GroupedRiskScores = Iterable[Tuple[Hashable, _RiskScores]]
_Contacts = Iterable[model.Contact]
log = backend.LOGGER


@attr.s(slots=True)
class BeliefPropagation:
	"""A factor graph that performs a variation of belief propagation to
	compute the propagated risk of exposure to a condition among a
	population. Factor nodes represent contacts between pairs of people,
	with node data containing all occurrences (time-duration pairs) in the
	recent past that two individuals came into sufficiently long contact.
	Variable nodes represent individuals, with node data containing the max
	risk score as well as all risk scores sent from neighboring factor
	nodes.

	Following the core msg-passing principle of belief propagation,
	the algorithm performs iterative computation between the factor and
	variable node sets. The algorithm begins with all variable nodes
	selecting their maximum local score and sending it to all of their
	neighboring factor nodes. Once this is done, all factor nodes filter the
	risk scores, based on when the individuals came into contact, and relays
	all risk scores sent from one individual to the other individual
	involved in the contact. This completes one iteration of the algorithm
	and is repeated until either a certain number of iterations has passed or
	the summed difference in variable risk scores from the previous iteration
	drops below a set tolerance, whichever condition is satisfied first.

	Variable node id:
		- HAT name

	Variable node values:
		- local risk scores
		- max risk score
		- risk scores received from neighboring factor nodes

	Factor node id:
		- HAT names of the contact

	Factor node values:
		- occurrences (time-duration pairs)

	Edge data:
		- msg between factor and variable nodes
	"""
	transmission_rate = attr.ib(type=float, default=0.8, converter=float)
	tolerance = attr.ib(type=float, default=1e-5, converter=float)
	iterations = attr.ib(type=int, default=4, converter=int)
	max_messages = attr.ib(type=Optional[int], default=None)
	backend = attr.ib(type=str, default=graphs.IGRAPH)
	_graph = attr.ib(type=FactorGraph, init=False)
	_vertex_store = attr.ib(type=graphs.VertexStore, init=False)
	_queue = attr.ib(type=queue.Queue, init=False)
	_seed = attr.ib(default=None)

	def __attrs_post_init__(self):
		if self.backend == graphs.IGRAPH:
			self._graph = graphs.IGraphFactorGraph()
		elif self.backend == graphs.NETWORKX:
			self._graph = graphs.NetworkXFactorGraph()
		self._vertex_store = graphs.VertexStore()
		max_messages = 0 if self.max_messages is None else self.max_messages
		self._queue = queue.Queue(maxsize=max_messages)
		if self._seed is not None:
			random.seed(self._seed)
			np.random.seed(self._seed)

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

	@max_messages.validator
	def _check_max_messages(self, attribute, value):
		if value is not None:
			if not isinstance(value, int):
				raise TypeError("'max_messages' must of type int or None")
			if value < 0:
				raise ValueError("'max_messages' must be least 1")

	def __call__(
			self,
			factors: _Contacts,
			variables: _GroupedRiskScores
	) -> Iterable[Tuple[Vertex, model.RiskScore]]:
		txt = 'BELIEF PROPAGATION: {:0.6f} s'
		with codetiming.Timer(text=txt, logger=log):
			return self.call(factors=factors, variables=variables)

	def call(
			self,
			factors: _Contacts,
			variables: _GroupedRiskScores
	) -> Iterable[Tuple[Vertex, model.RiskScore]]:
		log('-----------Start of algorithm-----------')
		with codetiming.Timer(text='Creating the graph: {:0.6f} s'):
			factors, variables = self._create_graph(factors, variables)
		get_maxes = functools.partial(
			BeliefPropagation._get_maxes.remote,
			vertex_store=self._vertex_store,
			variables=variables)
		maxes = np.array(ray.get(ray.get(get_maxes())))
		i, t = 0, np.inf
		while i < self.iterations and t > self.tolerance:
			log(f'-----------Iteration {i + 1}-----------')
			with codetiming.Timer(text='To factors: {:0.6f} s', logger=log):
				remaining = self._send_to_factors(variables)
				self._update_inboxes(remaining)
			with codetiming.Timer(text='To variables: {:0.6f} s', logger=log):
				remaining = self._send_to_variables(factors)
				self._update_inboxes(remaining)
			with codetiming.Timer(text='Updating: {:0.6f} s', logger=log):
				iter_maxes = ray.get(self._update_maxes.remote())
				t = np.sum(iter_maxes - maxes)
				maxes = iter_maxes
				i += 1
			log(f'Tolerance: {np.round(t, 6)}')
		log('-----------End of algorithm-----------')
		variables = np.nditer(ray.get(variables), order='C')
		maxes = ray.get(ray.get(get_maxes(order='C')))
		return (
			(v, model.RiskScore.from_array(m))
			for v, m in zip(variables, maxes))

	def _create_graph(
			self,
			factors: _Contacts,
			variables: _GroupedRiskScores) -> NoReturn:
		# Must create variables first before creating edges
		self._add_variables(variables)
		self._add_factors_and_edges(factors)
		factors = ray.put(np.array(list(self._graph.get_factors())))
		variables = ray.put(np.array(list(self._graph.get_variables())))
		# Only stores ray object reference, not the graph itself
		self._graph = ray.put(self._graph)
		return factors, variables

	def _add_variables(self, variables: _GroupedRiskScores):
		keys = []
		attrs = {}
		kv = (
			(str(k), np.array([v.as_array() for v in values]).flatten())
			for k, values in variables)
		for k, v in kv:
			keys.append(k)
			attrs.update({
				k: {
					'local': np.unique(v),
					'max': np.sort(v, order=['value', 'timestamp', 'id'])[-1],
					'inbox': {}}})
		self._graph.add_variables(keys)
		self._vertex_store.put(keys, attrs)

	def _add_factors_and_edges(self, factors: _Contacts):
		def make_key(factor: model.Contact):
			parts = [str(u) for u in sorted(factor.users)]
			key = '_'.join(parts)
			return key, parts

		vertices, edges = [], []
		attrs = {}
		for f in factors:
			k, (v1, v2) = make_key(f)
			vertices.append(k)
			attrs.update({k: {'occurrences': f.as_array(), 'inbox': {}}})
			e1, e2 = (k, v1), (k, v2)
			edges.extend((e1, e2))
		self._graph.add_factors(vertices)
		self._vertex_store.put(vertices, attrs)
		self._graph.add_edges(edges)

	@staticmethod
	@ray.remote
	def _get_maxes(
			vertex_store: graphs.VertexStore,
			variables: np.ndarray,
			order: str = 'K') -> Sequence[ray.ObjectRef]:
		get_max = functools.partial(
			vertex_store.get, attribute='max', as_ref=True)
		return [get_max(key=v) for v in np.nditer(variables, order=order)]

	def _send_to_factors(
			self, variables: ray.ObjectRef) -> Sequence[ray.ObjectRef]:
		shards = _get_index_batches(ray.get(variables)).shards()
		return [
			BeliefPropagation._to_factors.remote(
				graph=self._graph, vertex_store=self._vertex_store,
				variables=variables, msg_queue=self._queue,
				block_queue=self.max_messages is None, indices=shard)
			for shard in shards]

	@staticmethod
	@ray.remote
	def _to_factors(
			graph: FactorGraph,
			vertex_store: graphs.VertexStore,
			variables: np.ndarray,
			msg_queue: queue.Queue,
			block_queue: bool,
			indices: Iterable[np.ndarray]):
		for v in itertools.chain(*(variables[i] for i in indices)):
			inbox = vertex_store.get(key=v, attribute='inbox')
			local = vertex_store.get(key=v, attribute='local')
			for f in graph.get_neighbors(v):
				from_others = {o: msg for o, msg in inbox.items() if o != f}
				content = itertools.chain(local, from_others.values())
				msg = graphs.Message(sender=v, receiver=f, content=content)
				msg_queue.put(msg, block=block_queue)

	def _update_inboxes(self, remaining: Sequence[ray.ObjectRef]) -> NoReturn:
		# TODO Try updating inbox directly (by reference still?)
		while len(remaining) and self._queue.size():
			_, remaining = ray.wait(remaining)
			msg = self._queue.get()
			inbox = self._vertex_store.get(key=msg.receiver, attribute='inbox')
			inbox[msg.sender] = msg.content
			attrs = {msg.receiver: inbox}
			self._vertex_store.put(keys=[msg.receiver], attributes=attrs)

	def _send_to_variables(
			self, factors: ray.ObjectRef) -> Sequence[ray.ObjectRef]:
		shards = _get_index_batches(ray.get(factors)).shards()
		return [
			BeliefPropagation._to_variables.remote(
				graph=self._graph, vertex_store=self._vertex_store,
				factors=factors, msg_queue=self._queue,
				block_queue=self.max_messages is None, indices=shard,
				transmission_rate=self.transmission_rate)
			for shard in shards]

	# Must be positioned BEFORE _to_variables() for nested remote functions
	@staticmethod
	@ray.remote
	def _compute_message(
			vertex_store: graphs.VertexStore,
			factor: Vertex,
			messages: np.ndarray,
			transmission_rate: float) -> np.ndarray:
		"""Computes the message to send from a factor to a variable.

		Only messages that occurred sufficiently before at least one factor
		value are considered. Messages undergo a weighted transformation,
		based on the amount of time between the message's timestamp and the
		most recent message's timestamp and transmission rate. If no
		messages satisfy the initial condition, a defaults message is sent.
		Otherwise, the maximum weighted message is sent.

		Args:
			factor: Factor node sending the messages.
			messages: Messages in consideration.

		Returns:
			Message to send.
		"""
		# Occurrences are already sorted by timestamp, then duration
		occurrences = vertex_store.get(key=factor, attribute='occurrences')
		messages.sort(order=['timestamp', 'value', 'id'])
		m = np.where(messages <= np.max(occurrences['timestamp']) - _TWO_DAYS)
		old_enough = messages[m]
		if len(old_enough) == 0:
			msg = _DEFAULT_MESSAGE
		else:
			diff = np.timedelta64(old_enough['timestamp'] - _NOW, 'D')
			exp = np.exp(np.int16(diff))
			norm = np.cumsum(exp)
			weighted = np.cumsum(old_enough['value'] * exp)
			weighted *= transmission_rate / norm
			mx, ind = np.max(weighted), np.argmax(weighted)
			msg = old_enough[ind]
			msg_id, timestamp = msg['id'], msg['timestamp']
			msg = model.RiskScore(id=msg_id, timestamp=timestamp, value=mx)
		return msg.as_array()

	@staticmethod
	@ray.remote
	def _to_variables(
			graph: FactorGraph,
			factors: np.ndarray,
			vertex_store: graphs.VertexStore,
			msg_queue: queue.Queue,
			block_queue: bool,
			indices: Iterable[np.ndarray],
			transmission_rate: float):
		for f in itertools.chain(*(factors[i] for i in indices)):
			neighbors = np.array(list(graph.get_neighbors(f)))
			for i, v in enumerate(neighbors):
				# Assumes factor vertex has a degree of 2
				neighbor = neighbors[not i]
				local = vertex_store.get(key=neighbor, attribute='local')
				inbox = vertex_store.get(key=neighbor, attribute='inbox')
				messages = itertools.chain(local, inbox.values())
				messages = np.array([m for m in messages]).flatten()
				content = ray.get(BeliefPropagation._compute_message.remote(
					vertex_store=vertex_store, factor=f, messages=messages,
					transmission_rate=transmission_rate))
				msg = graphs.Message(sender=f, receiver=v, content=content)
				msg_queue.put(msg, block=block_queue)

	@ray.remote
	def _update_maxes(
			self,
			vertex_store: graphs.VertexStore,
			variables: np.ndarray) -> np.ndarray:
		updated = []
		for v in np.nditer(variables):
			inbox = vertex_store.get(key=v, attribute='inbox').values()
			mx = vertex_store.get(key=v, attribute='max')
			messages = np.array([m for m in itertools.chain(inbox, [mx])])
			messages = messages.flatten()
			mx = np.sort(messages, order=['value', 'timestamp', 'id'])[-1]
			vertex_store.put(keys=[v], attributes={v: {'max': mx}})
			updated.append(mx['value'])
		return np.array(updated).flatten()


def _get_index_batches(vertices: Sized) -> it.ParallelIterator:
	num_vertices = len(vertices)
	indices = it.from_range(num_vertices, num_shards=backend.NUM_CPUS)
	# Guarantees multiple batches for multiple CPUs and a max size of 100
	batch_size = min(100, int(np.ceil(num_vertices / backend.NUM_CPUS ** 2)))
	return indices.batch(batch_size).for_each(np.array)

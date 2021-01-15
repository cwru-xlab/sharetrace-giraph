import collections
import datetime
import functools
import itertools
import random
from typing import (
	Any, DefaultDict, Hashable, Iterable, Mapping, NoReturn, Set,
	Sized, Tuple)

import attr
import codetiming
import numpy as np
import ray
from ray.util import iter as it, queue

import backend
import graphs
import model
from backend import NUM_CPUS
from graphs import FactorGraph, Vertex

"""
Optimization best practices:
	- Use itertools over custom implementations
	- Use generators for intermediate processing results
	- Use numpy array over standard list or tuple
	- Only make copies of mutable collections if modifying them
"""

_TWO_DAYS = datetime.timedelta(days=2)
_DEFAULT_MESSAGE = model.RiskScore(
	id='DEFAULT_ID', timestamp=datetime.datetime.utcnow(), value=0)
_RiskScores = Iterable[model.RiskScore]
_GroupedRiskScores = Iterable[Tuple[str, Iterable[model.RiskScore]]]
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

	Following the core messages-passing principle of belief propagation,
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
		- messages between factor and variable nodes
	"""
	transmission_rate = attr.ib(type=float, default=0.8, converter=float)
	tolerance = attr.ib(type=float, default=1e-5, converter=float)
	iterations = attr.ib(type=int, default=4, converter=int)
	max_messages = attr.ib(type=int, default=None)
	backend = attr.ib(type=str, default=graphs.IGRAPH)
	_graph = attr.ib(type=FactorGraph, init=False)
	_queue = attr.ib(type=queue.Queue, init=False)
	_seed = attr.ib(default=None)

	def __attrs_post_init__(self):
		self._graph = graphs.RayFactorGraph(backend=self.backend).remote()
		max_messages = 0 if self.max_messages is None else self.max_messages
		self._queue = queue.Queue(maxsize=max_messages)
		if self._seed is not None:
			random.seed(self._seed)
			np.random.seed(self._seed)

	@transmission_rate.validator
	def _check_transmission_rate(self, attribute, value):
		if value < 0 or value > 1:
			raise ValueError(
				'Transmission rate must be between 0 and 1, inclusive')

	@tolerance.validator
	def _check_tolerance(self, attribute, value):
		if value <= 0:
			raise ValueError('Tolerance must be greater than 0')

	@iterations.validator
	def _check_iterations(self, attribute, value):
		if value < 1:
			raise ValueError('Iterations must be at least 1')

	@max_messages.validator
	def _check_max_messages(self, attribute, value):
		if attribute is not None:
			if not isinstance(attribute, int):
				raise TypeError('Max messages must of type int or None')
			if attribute < 0:
				raise ValueError('Max messages must be least 1')

	def __call__(
			self,
			factors: _Contacts,
			variables: _GroupedRiskScores
	) -> Iterable[Tuple[Vertex, model.RiskScore]]:
		txt = 'BELIEF PROPAGATION: {:0.6f} s'
		with codetiming.Timer(text=txt, logger=log):
			return self.call(factors=factors, variables=variables)

	# TODO IMPORTANT: If using a queue to store messages, then the node data
	#  is the only stateful component that can be stored separate from the
	#  graph itself. In this way, the graph can be built and pinned in memory
	#  to be shared by all tasks needing to access it. Thus, the Actor
	#  required is something like a VertexStore that maintains the state of
	#  each of the vertices (i.e. previous, local, and max)

	# TODO Design signal to switch between send_to_variables and
	#  send_to_factors
	# TODO Design Actor to store and update state of nodes
	# TODO Modify design to not store edge data
	# TODO Implement Message class

	def call(
			self,
			factors: _Contacts,
			variables: _GroupedRiskScores
	) -> Iterable[Tuple[Vertex, model.RiskScore]]:
		with codetiming.Timer(text='Creating the graph: {:0.6f} s'):
			self._create_graph(factors, variables)
		maxes = self._get_all_max()
		i, t = 0, np.inf
		log('-----------Start of algorithm-----------')
		while i < self.iterations and t > self.tolerance:
			log(f'-----------Iteration {i + 1}-----------')
			with codetiming.Timer(text='To factors: {:0.6f} s', logger=log):
				signal = None
				self._send_to_factors(signal)
			with codetiming.Timer(text='To variables: {:0.6f} s', logger=log):
				self._send_to_variables(signal)
			i += 1
			with codetiming.Timer(text='Updating: {:0.6f} s', logger=log):
				iter_maxes = self._update_max()
				t = sum(iter_maxes - maxes)
				maxes = iter_maxes
				if i < self.iterations:
					self._set_previous()
					self._clear_messages()
			log(f'Tolerance: {round(t, 6)}')
		log('-----------End of algorithm-----------')
		return ((v, self._get_max(v)) for v in self._graph.get_variables())

	def _create_graph(
			self,
			factors: _Contacts,
			variables: _GroupedRiskScores) -> NoReturn:
		# Must create variables first before creating edges
		self._add_variables(variables)
		self._add_factors_and_edges(factors)

	def _add_variables(self, variables: _GroupedRiskScores):
		keys = []
		attrs = {}
		for k, v in variables:
			v1, v2 = itertools.tee(v)
			keys.append(k)
			attrs.update({
				k: {
					'local': _local_factory(v1),
					'max': max(v2),
					'previous': _previous_factory()}})
		self._graph.add_vertices(keys, attrs)
		self._graph.set_variables(keys)

	def _add_factors_and_edges(self, factors: _Contacts):
		def make_key(factor: model.Contact):
			parts = [str(u) for u in sorted(factor.users)]
			key = '_'.join(parts)
			return key, parts

		vertices, edges = [], []
		vertex_attrs, edge_attrs = {}, {}
		for f in factors:
			k, (v1, v2) = make_key(f)
			vertices.append(k)
			vertex_attrs.update({k: {'occurrences': f.occurrences}})
			e1, e2 = (k, v1), (k, v2)
			edges.extend((e1, e2))
			# TODO Don't need this anymore
			edge_attrs.update({
				e1: _edge_data_factory(), e2: _edge_data_factory()})
		self._graph.set_factors(vertices)
		self._graph.add_vertices(vertices, vertex_attrs)
		self._graph.add_edges(edges, edge_attrs)

	def _send_to_variables(self, signal):  # TODO Signal
		factors = self._graph.get_factors()
		for shard in _get_index_batches(factors).shards():
			self._to_variables.remote(
				self._graph,
				self._queue,
				shard,
				self.transmission_rate,
				signal)

	@ray.remote
	def _to_variables(
			self,
			graph: FactorGraph,
			msg_queue: queue.Queue,
			indices: Iterable[np.ndarray],
			transmission_rate: float,
			signal):
		factors = graph.get_factors()
		for f in itertools.chain(*(factors[i] for i in indices)):
			neighbors = np.array(list(graph.get_neighbors(f)))
			for i, v in enumerate(neighbors):
				neighbor = neighbors[not i]
				local = self._graph.get_vertex_attr(
					vertex=neighbor, key='local')
				previous = self._get_previous(graph, neighbor)
				messages = itertools.chain(local, previous)
				message = self._compute_message(
					graph, f, messages, transmission_rate)
				# block = self.max_messages is None
				msg_queue.put({(f, v): message})

	def _compute_message(
			self,
			graph: FactorGraph,
			factor: Vertex,
			messages: _RiskScores,
			transmission_rate: float) -> model.RiskScore:
		"""Computes messages to send from a factor node to a variable node.

		Only messages that occurred sufficiently before at least one factor
		value are considered. Messages undergo a weighted transformation,
		based on the number of days between the message's timestamp and the
		most recent message's timestamp and transmission rate. If no
		messages satisfy the initial condition, a default message is sent.
		Otherwise, the maximum weighted message is sent.

		Args:
			factor: Factor node sending the messages.
			messages: Messages in consideration.

		Returns:
			Message to send.
		"""
		messages = np.array(
			sorted(messages, key=lambda m: m.timestamp, reverse=True))
		occurs = self._graph.get_vertex_attr(vertex=factor, key='occurrences')
		recent = np.array([
			(i, m) for i, m in enumerate(messages)
			if any(m.timestamp <= o.timestamp + _TWO_DAYS for o in occurs)])
		if len(recent) == 0:
			message = _DEFAULT_MESSAGE
		else:
			# TODO + 1 okay to avoid zero division?
			# TODO Is the original message sent or the weighted one?
			norms = (sum(np.exp(-d) for d in range(i + 1)) for i, _ in recent)
			weighted = (
				(i, sum(m.value * np.exp(-d) for d in range(i)))
				for i, m in recent)
			weighted = (
				(i, w * transmission_rate / norm)
				for (i, w), norm in zip(weighted, norms))
			i, _ = max(weighted, key=lambda x: x[1])
			message = messages[i]
		return message

	def _send_to_factors(self, signal):  # TODO Signal
		variables = self._graph.get_variables()
		indices = _get_index_batches(variables)
		for shard in indices.shards():
			self._to_factors.remote(
				self._graph, variables, self._queue, shard, signal)

	@ray.remote
	def _to_factors(
			self,
			graph: FactorGraph,
			variables: np.ndarray,
			msg_queue: queue.Queue,
			indices: Iterable[np.ndarray],
			signal):
		for v in itertools.chain(*(variables[i] for i in indices)):
			local = graph.get_vertex_attr(vertex=v, key='local')
			for f in graph.get_neighbors(v):
				from_others = self._get_from_other_factors(graph, f, v)
				messages = itertools.chain(local, from_others.values())
				# block = self.max_messages is None
				msg_queue.put({(f, v): messages})

	def _get_to_variable(
			self, variable: Vertex) -> Mapping[Vertex, model.RiskScore]:
		to_variable = _previous_factory()
		to_variable.update({
			f: self._graph.get_edge_attr(edge=(variable, f), key='to_variable')
			for f in self._graph.get_neighbors(variable)})
		return to_variable

	def _set_previous(self) -> NoReturn:
		for v in self._graph.get_variables():
			msg = self._get_to_variable(v)
			self._graph.set_vertex_attr(vertex=v, key='previous', value=msg)

	def _get_all_max(self) -> np.ndarray:
		variables = self._graph.get_variables()
		return np.array([self._get_max(v).value for v in variables])

	def _get_max(self, variable: Vertex) -> model.RiskScore:
		return self._graph.get_vertex_attr(vertex=variable, key='max')

	def _update_max(self) -> np.ndarray:
		updated = []
		for v in self._graph.get_variables():
			msg = self._get_to_variable(v).values()
			mx = self._get_max(v)
			mx = _chain_max(msg, [mx], key=lambda r: r.value)
			self._graph.set_vertex_attr(vertex=v, key='max', value=mx)
			updated.append(mx.value)
		return np.array(updated)

	def _clear_messages(self):
		variables = self._graph.get_variables()
		neighbors = (self._graph.get_neighbors(v) for v in variables)
		for v, f in itertools.product(variables, neighbors):
			self._graph.set_edge_attr(
				edge=(v, f), key='to_variable', value=None)
			self._graph.set_edge_attr(
				edge=(v, f), key='to_factor', value=_to_factor_factory())

	@staticmethod
	def _get_from_other_factors(
			graph: FactorGraph,
			factor: Vertex,
			variable: Vertex) -> Mapping[Vertex, model.RiskScore]:
		to_variable = _previous_factory()
		get_previous = functools.partial(BeliefPropagation._get_previous)
		to_variable.update({
			f: msg for f, msg in get_previous(graph, variable).items()
			if f != factor})
		return to_variable

	@staticmethod
	def _get_previous(
			graph: FactorGraph,
			variable: Vertex) -> Mapping[Vertex, model.RiskScore]:
		return graph.get_vertex_attr(vertex=variable, key='previous')


def _get_index_batches(vertices: Sized) -> it.ParallelIterator:
	num_vertices = len(vertices)
	indices = it.from_range(num_vertices, num_shards=NUM_CPUS)
	# Guarantees multiple batches for multiple CPUs and a max size of 100
	batch_size = int(np.ceil(num_vertices / NUM_CPUS ** 2))
	batch_size = min(100, batch_size)
	return indices.batch(batch_size).for_each(np.array)


def _previous_factory(values=None) -> DefaultDict[Vertex, model.RiskScore]:
	factory = collections.defaultdict(None)
	if values is not None:
		factory.update(values)
	return factory


def _to_factor_factory(values=None) -> Set[model.RiskScore]:
	return set(values) if values is not None else set()


def _local_factory(values=None) -> Set[model.RiskScore]:
	return set(values) if values is not None else set()


def _edge_data_factory() -> Mapping[Hashable, Any]:
	return {'to_variable': None, 'to_factor': _to_factor_factory()}


def _chain_max(*iterables, key=None, default=None):
	return max(itertools.chain(*iterables), key=key, default=default)

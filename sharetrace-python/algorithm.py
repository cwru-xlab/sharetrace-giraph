import collections
import datetime
import itertools
import random
from typing import (
	Any, DefaultDict, Generator, Hashable, Iterable, Mapping,
	NoReturn,
	Set, Tuple)

import attr
import numpy as np

import backend
import model

_TWO_DAYS = datetime.timedelta(days=2)
_DEFAULT_MESSAGE = model.RiskScore(
	id='DEFAULT_ID', timestamp=datetime.datetime.today(), value=0)
_RiskScores = Iterable[model.RiskScore]
_GroupedRiskScores = Iterable[Tuple[str, Iterable[model.RiskScore]]]
_Contacts = Iterable[model.Contact]

"""
Optimization best practices:
	- Use itertools over custom implementations
	- Use generators for intermediate processing results
	- Use numpy array over standard list or tuple
"""


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
	_backend = attr.ib(type=str, default=backend.IGRAPH)
	_graph = attr.ib(type=backend.FactorGraph, default=None)
	_seed = attr.ib(default=None)

	def __attrs_post_init__(self):
		if self._backend == backend.IGRAPH:
			self._graph = backend.IGraphFactorGraph()
		elif self._backend == backend.NETWORKX:
			self._graph = backend.NetworkXFactorGraph()
		else:
			raise AttributeError(
				f'Backend must be one of the following: {backend.OPTIONS}')
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

	def run(
			self,
			factors: _Contacts,
			variables: _GroupedRiskScores,
			as_generator: bool = True
	) -> Iterable[Tuple[Hashable, model.RiskScore]]:
		self._create_graph(factors, variables)
		max_risks = np.array(self._get_max_risks())
		iteration, tolerance = 0, np.inf
		while iteration < self.iterations and tolerance > self.tolerance:
			self._send_to_factors()
			self._send_to_variables()
			iteration += 1
			iter_risks = self._update_max_risks()
			tolerance = sum(iter_risks - max_risks)
			max_risks = iter_risks
			if iteration < self.iterations:
				self._copy_to_previous()
				self._clear_messages()
		risks = ((v, self._get_max_risk(v)) for v in self._graph.variables)
		return risks if as_generator else tuple((k, risk) for k, risk in risks)

	def _update_max_risks(self) -> np.ndarray:
		updated = []
		for v in self._graph.variables:
			messages = self._get_messages_to_variable(v).values()
			max_risk = self._get_max_risk(v)
			max_risk = _chain_max(messages, [max_risk], key=lambda r: r.value)
			self._set_max_risk(v, max_risk)
			updated.append(max_risk.value)
		return np.array(updated)

	def _create_graph(
			self,
			factors: _Contacts,
			variables: _GroupedRiskScores) -> NoReturn:
		# Must create variable vertices first before creating edges
		self._add_variables(variables)
		self._add_factors(factors)

	def _add_factors(self, factors: _Contacts) -> NoReturn:
		def make_key(factor: model.Contact):
			parts = [str(u) for u in sorted(factor.users)]
			key = '_'.join(parts)
			return key, parts

		factors = np.array([(make_key(f), f) for f in factors])
		keys = frozenset(k for ((k, _), _) in factors)
		self._graph.factors = keys
		attrs = {k: {'occurrences': f.occurrences} for ((k, _), f) in factors}
		self._graph.add_vertices(keys, attrs)
		edges = itertools.chain(
			((p, k) for ((k, (p, _)), _) in factors),
			((p, k) for ((k, (_, p)), _) in factors))
		edges1, edges2 = itertools.tee(edges)
		attrs = {e: _edge_data_factory() for e in edges1}
		self._graph.add_edges(edges2, attrs)

	def _add_variables(self, variables: _GroupedRiskScores) -> NoReturn:
		variables = np.array(
			[(str(k), np.array(list(v))) for k, v in variables])
		keys = frozenset(k for k, _ in variables)
		self._graph.variables = keys
		attrs = {
			k: {
				'local_risks': _local_factory(v),
				'max_risk': max(v),
				'previous': _previous_factory()}
			for k, v in variables}
		self._graph.add_vertices(keys, attrs)

	def _send_to_variables(self) -> NoReturn:
		for factor, variable in self._graph_iter(self._graph.factors):
			neighbor = self._get_neighbor(factor, variable)
			local = self._get_local_values(neighbor)
			previous = self._get_previous(neighbor).values()
			messages = itertools.chain(local, previous)
			message = self._compute_message(factor, messages)
			self._send_to_variable(factor, variable, message)

	def _graph_iter(
			self,
			outer: Iterable[Hashable]
	) -> Generator[Tuple[Hashable, Hashable], None, None]:
		"""Generates node-neighbor tuples.

		A cartesian product of the outer iterable with neighbors of each of
		the outer iterable elements is iterated over.

		Args:
			outer: Elements over which to iterate and generate neighbors.

		Returns:
			A generator with each element being a tuple of the form (outer
			element, neighbor of outer element).
		"""
		for o in outer:
			for n in self._graph.get_neighbors(o):
				yield o, n

	def _compute_message(
			self, factor: Hashable, messages: _RiskScores) -> NoReturn:
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
		value = self._get_factor_value(factor)
		recent = np.array([
			(i, m) for i, m in enumerate(messages)
			if any(m.timestamp <= v.timestamp + _TWO_DAYS for v in value)])
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
				(i, w * self.transmission_rate / norm)
				for (i, w), norm in zip(weighted, norms))
			i, _ = max(weighted, key=lambda x: x[1])
			message = messages[i]
		return message

	def _send_to_factors(self) -> NoReturn:
		for variable, factor in self._graph_iter(self._graph.variables):
			local = self._get_local_values(variable)
			from_others = self._get_from_other_factors(factor, variable)
			messages = itertools.chain(local, from_others.values())
			self._send_to_factor(factor, variable, messages)

	# Edge getters / setters
	def _get_messages_to_variable(
			self, variable: Hashable) -> Mapping[Hashable, model.RiskScore]:
		to_variable = _previous_factory()
		to_variable.update({
			f: self._graph.get_edge_attr(edge=(variable, f), key='to_variable')
			for f in self._graph.get_neighbors(variable)})
		return to_variable

	def _get_from_other_factors(
			self,
			factor: Hashable,
			variable: Hashable) -> Mapping[Hashable, model.RiskScore]:
		"""Get all messages received by a variable node from all other of
		its neighboring factor nodes."""
		to_variable = _previous_factory()
		to_variable.update({
			f: msg for f, msg in self._get_previous(variable).items()
			if f != factor})
		return to_variable

	def _send_to_factor(
			self,
			factor: Hashable,
			variable: Hashable,
			messages: Iterable[model.RiskScore]) -> NoReturn:
		edge = (variable, factor)
		value = self._graph.get_edge_attr(edge=edge, key='to_factor')
		value.update(messages)
		self._graph.set_edge_attr(edge=edge, key='to_factor', value=value)

	def _send_to_variable(
			self,
			factor: Hashable,
			variable: Hashable,
			message: model.RiskScore) -> NoReturn:
		self._graph.set_edge_attr(
			edge=(variable, factor), key='to_variable', value=message)

	def _copy_to_previous(self) -> NoReturn:
		for v in self._graph.variables:
			messages = self._get_messages_to_variable(v)
			self._set_previous(v, messages)

	def _clear_messages(self):
		for v, f in self._graph_iter(self._graph.variables):
			self._graph.set_edge_attr(
				edge=(v, f), key='to_variable', value=None)
			self._graph.set_edge_attr(
				edge=(v, f), key='to_factor', value=_to_factor_factory())

	# Factor node getters / setters
	def _get_factor_value(
			self, factor: Hashable) -> Iterable[model.Occurrence]:
		return self._graph.get_vertex_attr(vertex=factor, key='occurrences')

	# Variable node getters / setters
	def _get_neighbor(self, factor: Hashable, variable: Hashable) -> Hashable:
		neighbors = iter(self._graph.get_neighbors(factor))
		neighbor = next(neighbors)
		return neighbor if neighbor != variable else next(neighbors)

	def _get_local_values(
			self, variable: Hashable) -> Iterable[model.RiskScore]:
		local = self._graph.get_vertex_attr(vertex=variable, key='local_risks')
		return _local_factory(local)

	def _get_previous(
			self, variable: Hashable) -> Mapping[Hashable, model.RiskScore]:
		previous = self._graph.get_vertex_attr(vertex=variable, key='previous')
		return _previous_factory(previous)

	def _set_previous(
			self,
			variable: Hashable,
			value: Mapping[Hashable, model.RiskScore]) -> NoReturn:
		self._graph.set_vertex_attr(
			vertex=variable, key='previous', value=value)

	def _get_max_risks(self) -> Iterable[model.RiskScore]:
		max_risks = (
			self._get_max_risk(v).value for v in self._graph.variables)
		return np.array(list(max_risks))

	def _get_max_risk(self, variable: Hashable) -> model.RiskScore:
		return self._graph.get_vertex_attr(vertex=variable, key='max_risk')

	def _set_max_risk(
			self, variable: Hashable, value: model.RiskScore) -> NoReturn:
		self._graph.set_vertex_attr(
			vertex=variable, key='max_risk', value=value)


def _previous_factory(values=None) -> DefaultDict[Hashable, model.RiskScore]:
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

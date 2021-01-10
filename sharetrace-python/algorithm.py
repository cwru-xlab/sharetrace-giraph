import datetime
import itertools
import random
from typing import Collection, Generator, Hashable, Iterable, NoReturn

import attr
import networkx as nx
import numpy as np

import model

_TWO_DAYS = datetime.timedelta(days=2)
_DEFAULT_ID = 'DEFAULT_ID'
_DEFAULT_RISK = 0
_DEFAULT_DURATION = 0

"""
Variable node id: HAT name
Variable node messages: set of local risk scores and max risk score

Factor node id: Concatenation of HAT names
Factor node messages: Occurrences

Edge data: Messages between factor and variable nodes
"""


@attr.s(slots=True)
class BeliefPropagation(nx.Graph):
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
	"""
	transmission_rate = attr.ib(type=float, default=0.8, converter=float)
	tolerance = attr.ib(type=float, default=1e-5, converter=float)
	iterations = attr.ib(type=int, default=4, converter=int)
	_seed = attr.ib(default=None)

	def __attrs_post_init__(self):
		if self._seed is not None:
			random.seed(self._seed)
			np.random.seed(self._seed)

	@transmission_rate.validator
	def _check_transmission_rate(self, attribute, value):
		if value < 0 or value > 1:
			raise ValueError('Transmission rate must be between 0 and 1')

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
			factors: Collection[model.Contact],
			variables: Collection[model.RiskScore]
	) -> Collection[model.RiskScore]:
		self._create_graph(factors, variables)
		max_risks = np.array([
			self._get_max_risk(v).value for v in self._variables])
		iteration, tolerance = 0, np.inf
		while iteration < self.iterations and tolerance > self.tolerance:
			iter_risks = []
			self._send_to_factors()
			self._send_to_variables()
			fv = self._graph_iter(self._factors, filter_neighbors=False)
			for factor, variable in fv:
				message = self._get_message_to_variable(factor, variable)
				max_risk = self._get_max_risk(variable)
				self._set_max_risk(variable, max(message, max_risk))
				iter_risks.append(max_risk.value)
			iteration += 1
			tolerance = sum(np.array(iter_risks) - max_risks)
			max_risks = iter_risks
			if iteration < self.iterations:
				self._clear_messages()
		return tuple(self._get_max_risk(v) for v in self._variables)

	def _create_graph(
			self,
			factors: Collection[model.Contact],
			variables: Collection[model.RiskScore]) -> NoReturn:
		self._add_factors(factors)
		self._add_variables(variables)

	def _add_factors(
			self, factors: Collection[model.Contact]) -> NoReturn:
		users = [sorted(f.users) for f in factors]
		keys = tuple(f'{u1}_{u2}' for u1, u2 in users)
		self._factors = keys
		self.add_nodes_from(keys, bipartite=1)
		attrs = {
			k: {'occurrences': f.occurrences} for f, k in zip(factors, keys)}
		nx.set_node_attributes(self, attrs)
		edge_data = {'to_variable': set(), 'to_factor': set()}
		edges = ((u, k) for (u, _), k in zip(users, keys))
		self.add_edges_from(edges, **edge_data)
		edges = ((u, k) for (_, u), k in zip(users, keys))
		self.add_edges_from(edges, **edge_data)

	def _add_variables(
			self, variables: Collection[model.RiskScore]) -> NoReturn:
		keys = tuple(v.id for v in variables)
		self._variables = keys
		self.add_nodes_from(keys, bipartite=0)
		attrs = {v.id: {'local_risks': {v}, 'max_risk': v} for v in variables}
		nx.set_node_attributes(self, attrs)

	def _get_local_values(
			self, variable: Hashable) -> Collection[model.RiskScore]:
		return self.nodes[variable]['local_risks']

	def _set_max_risk(self, variable_id, new_max) -> NoReturn:
		self.nodes[variable_id]['max_risk'] = new_max

	def _send_to_variables(self) -> NoReturn:
		for factor, variable, neighbor in self._graph_iter(self._factors):
			from_neighbor = self._get_local_values(neighbor)
			from_others = self._get_from_other_factors(factor, variable)
			messages = itertools.chain(from_neighbor, from_others)
			self._compute_message(factor, variable, messages)

	def _graph_iter(
			self, outer: Iterable, filter_neighbors: bool = True) -> Generator:
		"""Generates factor, variable, and (optionally) neighbor tuples.

		A cartesian product is iterated over the outer iterable and the inner
		iterable that is created from the neighbors of each of the elements of
		the outer iterable.

		Args:
			outer: Defines the outer for loop over which the neighbors of
				each element of the outer iterable are iterated over in the
				inner for loop. For example, if an iterable of factor nodes,
				the inner iterable is an iterable such that each element in
				the iterable is the neighbors of a factor node.
			filter_neighbors: If True, a third level of iteration is defined
				in which the neighbors of the current outer element are
				iterated over, except for the current inner element. If
				False, this level of iteration is ignored, and only the
				outer-inner iteration is performed.

		Returns:
			A tuple of either the form (outer element, inner element) or
			(outer element, inner element, outer neighbor), depending on the
			messages of filter_neighbors.
		"""
		inner = (self.neighbors(o) for o in outer)
		for o, i in itertools.product(outer, inner):
			if filter_neighbors:
				for n in (n for n in self.neighbors(o) if n != i):
					yield o, i, n
			else:
				yield o, i

	def _get_from_other_factors(
			self,
			factor: Hashable,
			variable: Hashable) -> Collection[model.RiskScore]:
		"""Get all messages received by a variable node from all other of
		its neighboring factor nodes."""
		return frozenset(
			self[variable][f]['to_variable']
			for f in self._factors if f != factor)

	def _compute_message(
			self,
			factor: Hashable,
			variable: Hashable,
			messages: Iterable[model.RiskScore]) -> NoReturn:
		"""Computes the messages to send from a factor node to a variable
		node.

		Sufficiently recent factor node values are filtered and the
		timestamp
		of the most recent of those values becomes the timestamp for the
		messages to send to the variable node. The value to send is adjusted
		by the set transmission rate of the model. If no such factor
		value is available, a default value is sent in the messages.

		Args:
			factor: Factor node sending the messages.
			variable: Variable node receiving the messages.
			messages: Contains the value of the messages.

		Returns:
			True if the non-default messages is sent, and False otherwise.
		"""
		# Numpy array for efficiency
		messages = np.array(
			sorted(messages, key=lambda m: m.timestamp, reverse=True))
		value = self._get_factor_value(factor)
		recent = np.array([
			(i, m) for i, m in enumerate(messages)
			if any(m.timestamp <= v.timestamp + _TWO_DAYS for v in value)])
		if len(recent) == 0:
			message = model.RiskScore(
				id=_DEFAULT_ID,
				timestamp=datetime.datetime.today(),
				value=_DEFAULT_RISK)
		else:
			norms = (sum(np.exp(-d) for d in range(i)) for i, _ in recent)
			weighted = (
				(i, sum(m.value * np.exp(-d) for d in range(i)))
				for i, m in recent)
			weighted = (
				(i, w * self.transmission_rate / norm)
				for (i, w), norm in zip(weighted, norms))
			i, _ = max(weighted, key=lambda i, w: w)
			message = messages[i]
		self._send_to_variable(factor, variable, message)

	def _send_to_factors(self) -> NoReturn:
		for variable, factor, neighbor in self._graph_iter(self._variables):
			local = self._get_local_values(variable)
			# TODO Need to know which message NOT to send to avoid self-bias
			# TODO When to update local values and clear all messages?
			messages = []
			self._send_to_factor(factor, variable, messages)

	def _get_factor_value(
			self, factor: Hashable) -> Collection[model.Occurrence]:
		return frozenset(self.nodes[factor]['occurrences'])

	def _get_max_risk(self, variable: Hashable) -> model.RiskScore:
		return self.nodes[variable]['max_risk']

	def _get_message_to_variable(
			self, factor: Hashable, variable: Hashable) -> model.RiskScore:
		return self[variable][factor]['to_variable']

	def _get_message_to_factor(
			self,
			factor: Hashable,
			variable: Hashable) -> Collection[model.RiskScore]:
		return self[variable][factor]['to_factor']

	def _send_to_variable(
			self,
			factor: Hashable,
			variable: Hashable,
			message: model.RiskScore) -> NoReturn:
		self[variable][factor]['to_variable'].add(message)

	def _send_to_factor(
			self,
			factor: Hashable,
			variable: Hashable,
			messages: Collection[model.RiskScore]) -> NoReturn:
		self[variable][factor]['to_factor'].update(messages)

	def _clear_messages(self):
		for f, v in self._graph_iter(self._factors, filter_neighbors=False):
			self[v][f]['to_variable'].clear()
			self[v][f]['to_factor'].clear()

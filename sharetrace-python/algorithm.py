import datetime
import itertools
import random
from typing import Dict, Generator, Iterable, NoReturn, Sequence, \
	Tuple

import attr
import networkx as nx
import numpy as np

import model

_TWO_DAYS = datetime.timedelta(days=2)

"""
Variable node id: HAT name
Variable node value: set of local risk scores and max risk score

Factor node id: Concatenation of HAT names
Factor node value: Occurrences

Edge data: Messages between factor and variable_id nodes
"""


# TODO Need to include current time

@attr.s(slots=True)
class BeliefPropagation(nx.Graph):
	"""A factor graph that performs a variation of belief propagation to
	compute the propagated risk of exposure to a condition among a
	population. Factor nodes represent contacts between pairs of people,
	with node data containing all occurrences (time-duration pairs) in the
	recent past that two individuals came into sufficiently long contact.
	Variable nodes represent individuals, with node data containing the max
	risk score as well as all risk scores sent from neighboring factor nodes.

	Following the core message-passing principle of belief propagation,
	the algorithm performs iterative computation between the factor and
	variable_id node sets. The algorithm begins with all variable_id nodes
	selecting their maximum local score and sending it to all of their
	neighboring factor nodes. Once this is done, all factor nodes filter the
	risk scores, based on when the individuals came into contact, and relays
	all risk scores sent from one individual to the other individual
	involved in the contact. This completes one iteration of the algorithm
	and is repeated until either a certain number of iterations has passed or
	the summed difference in variable_id risk scores from the previous iteration
	drops below a set tolerance, whichever condition is satisfied first.
	"""
	factors = attr.ib(type=Sequence[model.Contact])
	variables = attr.ib(type=Sequence[model.RiskScore])
	transmission_rate = attr.ib(type=float, default=0.8)
	tolerance = attr.ib(type=float, default=1e-5)
	iterations = attr.ib(type=int, default=4)
	_seed = attr.ib(default=None)

	def __attrs_post_init__(self):
		# Factor nodes
		users = [sorted(f.users) for f in self.factors]
		keys = tuple(f'{u1}_{u2}' for u1, u2 in users)
		self._factor_ids = keys
		self.add_nodes_from(keys, bipartite=1)
		attrs = {
			k: {'occurrences': f.occurrences}
			for f, k in zip(self.factors, keys)}
		nx.set_node_attributes(self, attrs)
		edge_data = {'to_variable': set(), 'to_factor': set()}
		edges = ((u, k) for (u, _), k in zip(users, keys))
		self.add_edges_from(edges, **edge_data)
		edges = ((u, k) for (_, u), k in zip(users, keys))
		self.add_edges_from(edges, **edge_data)
		# Variable nodes
		keys = tuple(v.id for v in self.variables)
		self._variable_ids = keys
		self.add_nodes_from(keys, bipartite=0)
		attrs = {v.id: {'local_risks': {v}} for v in self.variables}
		nx.set_node_attributes(self, attrs)
		# Seed
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

	def run(self) -> Sequence[model.RiskScore]:
		max_risks = self._get_and_set_max_local_risks()
		i, t = 0, np.inf
		while i < self.iterations and t > self.tolerance:
			current_risks = []
			self._send_to_factors()
			self._send_to_variables()
			vf = self._graph_iter(self.variables, filter_neighbors=False)
			for variable, factor in vf:
				message = self._get_message_to_variable(factor, variable)
				message_date, _ = _get_first(message)
				variable_date, _ = _get_first(self._get_max_risk(variable))
				if message_date > variable_date:
					new_max = self._get_message_to_factor(factor, variable)
					self._set_max_risk(variable, new_max)
				risk, _ = _get_first(self._get_max_risk(variable))
				current_risks = np.append(current_risks, risk)
			i += 1
			t = sum(current_risks - max_risks)
			max_risks = current_risks
		return tuple(_get_first(self._get_max_risk(v)) for v in self.variables)

	def _get_and_set_max_local_risks(self) -> Sequence[model.RiskScore]:
		"""Sets the max risk for each variable_id node, based on local scores.

		Risk scores are sorted first by magnitude and then by time in
		descending order. An edge between each variable_id node and all of its
		neighboring factor nodes is added, which serves to store messages
		between each pair of nodes.

		Returns:
			Sequence of local variable_id node max risk scores.
		"""
		max_risks = []
		for variable in self._variable_ids:
			local_risks = self._get_local_risks(variable)
			max_risk_score = max(local_risks)
			self._set_max_risk(variable, max_risk_score)
			max_risks.append(max_risk_score)
		return max_risks

	def _get_local_risks(self, variable_id) -> Sequence:
		return self.nodes[variable_id]['local_risks']

	def _set_max_risk(self, variable_id, new_max) -> NoReturn:
		self.nodes[variable_id]['max_risk'] = new_max

	def _send_to_variables(self):
		for factor, variable, neighbor in self._graph_iter(self.factors):
			local_risks = self._get_local_risks(neighbor)
			from_others = self._get_from_other_factors(factor, variable)
			risk_scores = _merge_dicts(local_risks, from_others).items()
			risk_scores = sorted(risk_scores)
			risk_scores = _generate_risk_scores(risk_scores)
			risk_score = next(risk_scores)
			updated = False
			while not updated and risk_score is not None:
				updated = self._compute_message(factor, variable, risk_score)
				risk_score = next(risk_scores, None)

	def _graph_iter(
			self, outer: Iterable, filter_neighbors: bool = True) -> Generator:
		"""Generates factor, variable_id, and (optionally) neighbor tuples.

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
			value of filter_neighbors.
		"""
		inner = (self.neighbors(o) for o in outer)
		for o, i in itertools.product(outer, inner):
			if filter_neighbors:
				for n in (n for n in self.neighbors(o) if n != i):
					yield o, i, n
			else:
				yield o, i

	def _get_from_other_factors(self, factor: str, variable: str):
		"""Get all messages received by a variable_id node from all other of its
		neighboring factor nodes."""
		return self[factor][variable]['m_vf']

	def _compute_message(
			self, factor: str, variable: str, value: model.RiskScore) -> bool:
		"""Computes the message to send from a factor node to a variable_id node.

		Sufficiently recent factor node values are filtered and the timestamp
		of the most recent of those values becomes the timestamp for the
		message to send to the variable_id node. The value to send is adjusted
		by the set transmission rate of the model. If no such factor value is
		available, a default value is sent in the message.

		Args:
			factor: Factor node sending the message.
			variable: Variable node receiving the message.
			value: Contains the value of the message.

		Returns:
			True if the non-default message is sent, and False otherwise.
		"""
		date, risk = value
		values = self._get_factor_values(factor)
		recent = ((t, d) for (t, d) in values if date <= t + _TWO_DAYS)
		# TODO Need to also account for duration
		default = (date, 0)
		max_recent = max(recent, key=lambda t, d: -t, default=default)
		updated = max_recent != default
		msg = {max_recent[0]: risk * self.transmission_rate if updated else 0}
		self.add_edges_from([(variable, factor, {'m_fv': msg})])
		return updated

	def _send_to_factors(self):
		for variable, factor, neighbor in self._graph_iter(self._variable_ids):
			message = self._get_message_to_variable(neighbor, variable)
			self._send_to_factor(factor, variable, message)

	def _get_factor_values(self, factor) -> Iterable[Tuple]:
		vals = (v.split(';') for v in self.nodes[factor]['cont_inf'])
		return (
			(_to_date(t), datetime.timedelta(seconds=int(d))) for t, d in vals)

	def _get_max_risk(self, variable):
		return self.nodes[variable]['max_risk']

	def _get_message_to_variable(self, factor, variable) -> model.RiskScore:
		return self[variable][factor]['to_variable']

	def _get_message_to_factor(self, factor, variable) -> model.RiskScore:
		return self[variable][factor]['to_factor']

	def _send_to_variable(self, factor, variable, message) -> NoReturn:
		self[variable][factor]['to_variable'].add(message)

	def _send_to_factor(self, factor, variable, message) -> NoReturn:
		self[variable][factor]['to_factor'].clear()
		self[variable][factor]['to_factor'].update(message)


def _generate_risk_scores(
		risk_scores: Iterable[Tuple[float, str]]) -> Generator:
	score_iter = iter(risk_scores)
	risk_score = next(score_iter, None)
	while risk_score is not None:
		time, risk = risk_score
		yield float(risk), _to_date(time)
		risk_score = next(score_iter, None)


def _to_date(s: str) -> datetime.date:
	return datetime.datetime.strptime(s, _DATE_FORMAT).date()


def _get_first(d: Dict) -> Tuple:
	k = next(d, None)
	return (k, d[k]) if k is not None else (None, None)


def _merge_dicts(d1: Dict, d2: Dict):
	d1_join = {k: d1[k] if k not in d2 else max(d1[k], d2[k]) for k, v in d1}
	d2_diff = {k: d2[k] for k in d2 if k not in d1_join}
	return {**d1_join, **d2_diff}

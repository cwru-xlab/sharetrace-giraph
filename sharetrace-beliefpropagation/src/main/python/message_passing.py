import datetime
import itertools
import random
from typing import Dict, Generator, Iterable, NoReturn, Optional, Sequence, \
	Tuple

import attr
import networkx as nx
import numpy as np

random.seed(20)

TWO_DAYS = datetime.timedelta(days=2)
DATE_FORMAT = '%d/%m/%Y'
RiskScore = Tuple[float, datetime.date]


@attr.s(slots=True, frozen=True)
class BeliefPropagation:
	graph = attr.ib(type=nx.Graph)
	factors = attr.ib(type=Sequence)
	variables = attr.ib(type=Sequence)
	transmission_rate = attr.ib(type=float, default=0.8)
	tolerance = attr.ib(type=float, default=1e-5)
	iterations = attr.ib(type=int, default=4)

	@transmission_rate.validator
	def check_transmission_rate(self, attribute, value):
		if value < 0 or value > 1:
			raise ValueError('Transmission rate must be between 0 and 1')

	@tolerance.validator
	def check_tolerance(self, attribute, value):
		if value <= 0:
			raise ValueError('Tolerance must be greater than 0')

	@iterations.validator
	def check_iterations(self, attribute, value):
		if value < 1:
			raise ValueError('Iterations must be at least 1')

	def compute_message(self, factor, variable, risk_score: RiskScore) -> bool:
		"""Compute the message with respect to each contact between the two
		users and pick the maximum value"""
		risk, date = risk_score
		values = self.get_factor_values(factor)
		recent = ((t, d) for (t, d) in values if date <= t + TWO_DAYS)
		# TODO Need to also account for duration
		default = (date, 0)
		most_recent, _ = max(recent, key=lambda t, d: -t, default=default)
		updated = most_recent != default
		msg = {
			most_recent: risk * self.transmission_rate if updated else 0}
		self.graph.add_edges_from([(variable, factor, {'m_fv': msg})])
		return updated

	def send_to_variables(self):
		for factor, variable, neighbor in self.factor_graph_iter(self.factors):
			local_risks = self.get_local_risks(neighbor)
			from_others = self.get_from_other_factors(factor, variable)
			risk_scores = self.merge_dicts(local_risks, from_others).items()
			risk_scores = sorted(risk_scores, key=self.sort_by_risk_then_time)
			risk_scores = self.generate_risk_scores(risk_scores)
			risk_score = next(risk_scores)
			updated = False
			while not updated and risk_score is not None:
				updated = self.compute_message(factor, variable, risk_score)
				risk_score = next(risk_scores, None)

	def factor_graph_iter(
			self, outer: Iterable, filter_neighbors: bool = True) -> Tuple:
		inner = (self.graph.neighbors(o) for o in outer)
		for o, i in itertools.product(outer, inner):
			if filter_neighbors:
				for n in (n for n in self.graph.neighbors(o) if n != i):
					yield o, i, n
			else:
				yield o, i

	def max_local_risk(self) -> np.ndarray:
		old_risks = []
		for variable in self.variables:
			local_risks = self.get_local_risks(variable).items()
			max_date, max_risk = max(
				local_risks, key=self.sort_by_risk_then_time)
			self.update_max_risk(variable, {max_date: max_risk})
			old_risks.append(max_risk)
			for factor in self.graph.neighbors(variable):
				self.graph.add_edges_from([(variable, factor, {'m_vf': {}})])
		return np.array(old_risks)

	def send_to_factors(self):
		vfn = self.factor_graph_iter(self.variables)
		for variable, factor, neighbor in vfn:
			# store in m_vf all the incoming message to variable node v except
			# for the one it received from factor node f to avoid self-bias
			message = self.get_message(neighbor, variable, to_variable=True)
			self.send_message(factor, variable, message)

	def run(self):
		old_risks = self.max_local_risk()
		i = 0
		t = np.inf
		while i < self.iterations and t > self.tolerance:
			curr_risk = np.array([])
			self.send_to_variables()
			self.send_to_factors()
			vf = self.factor_graph_iter(self.variables, filter_neighbors=False)
			for variable, factor in vf:
				msg = self.get_message(factor, variable, to_variable=True)
				_, msg_date = self.get_first(msg)
				_, var_date = self.get_first(self.get_max_risk(variable))
				if msg_date > var_date:
					new_val = self.get_message(factor, variable)
					self.update_max_risk(variable, new_val)
				risk, _ = self.get_first(self.get_max_risk(variable))
				curr_risk = np.append(curr_risk, risk)
			i += 1
			t = sum(curr_risk - old_risks)
			old_risks = curr_risk
		# final risks
		for variable in self.variables:
			_, final_risk = self.get_first(self.get_max_risk(variable))

	def get_factor_values(self, factor) -> Iterable[Tuple]:
		vals = (v.split(';') for v in self.graph.nodes[factor]['cont_inf'])
		return (
			(self.to_date(t), datetime.timedelta(seconds=int(d)))
			for t, d in vals)

	def get_max_risk(self, variable):
		return self.graph.nodes[variable]['max_risk']

	def get_message(self, factor, variable, to_variable: bool = False) -> Dict:
		if to_variable:
			message = self.graph[variable][factor]['m_fv']
		else:
			message = self.graph[variable][factor]['m_vf']
		return message

	def send_message(
			self,
			factor,
			variable,
			message: Dict,
			to_variable: bool = False) -> NoReturn:
		if to_variable:
			self.graph[variable][factor]['m_fv'].update(message)
		else:
			self.graph[variable][factor]['m_vf'].update(message)

	def update_max_risk(self, variable, new_max: Dict) -> NoReturn:
		self.graph.nodes[variable]['max_risk'] = new_max

	def get_local_risks(self, variable) -> Dict:
		return self.graph.nodes[variable]['local_risks']

	def get_from_other_factors(self, factor, variable):
		return self.graph[factor][variable]['m_vf']

	@staticmethod
	def sort_by_risk_then_time(risk_score: RiskScore) -> Tuple:
		risk, time = risk_score
		return -risk, time

	@staticmethod
	def generate_risk_scores(
			risk_scores: Iterable[Tuple[float, str]]) -> Generator:
		score_iter = iter(risk_scores)
		risk_score = next(score_iter, None)
		while risk_score is not None:
			risk, time = risk_score
			yield float(risk), BeliefPropagation.to_date(time)
			risk_score = next(score_iter, None)

	@staticmethod
	def get_first(d: Dict) -> Optional[Tuple]:
		k = next(d, None)
		return (k, d[k]) if k is not None else None

	@staticmethod
	def merge_dicts(d1: Dict, d2: Dict):
		d1_join = {
			k: d1[k] if k not in d2 else max(d1[k], d2[k]) for k, v in d1}
		d2_diff = {k: d2[k] for k in d2 if k not in d1_join}
		return {**d1_join, **d2_diff}

	@staticmethod
	def to_date(s: str) -> datetime.date:
		return datetime.datetime.strptime(s, DATE_FORMAT).date()

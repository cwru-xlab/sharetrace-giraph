import abc
import datetime
import itertools
import numbers
import random
from typing import (
	Any, Dict, Hashable, Iterable, NoReturn, Optional, Tuple, Union
)

import attr
import codetiming
import numpy as np
from attr import validators

import backend
import graphs
import model
import stores

# Globals
_TWO_DAYS = np.timedelta64(2, 'D')
_DEFAULT_MESSAGE = model.RiskScore(
	name='DEFAULT_ID', timestamp=backend.TIME, value=0)
stdout = backend.STDOUT
stderr = backend.STDERR
# Types
TimestampBuffer = Union[datetime.timedelta, np.timedelta64]
RiskScores = Iterable[model.RiskScore]
AllRiskScores = Iterable[Tuple[Hashable, RiskScores]]
Contacts = Iterable[model.Contact]
Result = Iterable[Tuple[Hashable, model.RiskScore]]
Maxes = Union[np.ndarray, Iterable[Tuple[graphs.Vertex, model.RiskScore]]]


@attr.s(slots=True)
class BeliefPropagation(abc.ABC):
	"""Runs the belief propagation algorithm to compute exposure scores.

	A factor vertex represents a pair of people, with vertex data containing
	all occurrences (time-duration pairs) in the recent past that the two
	individuals came into sufficiently long contact. A variable vertex
	represents an individual, with vertex data containing the local risk
	scores that are derived from user-reported symptoms, the maximum of these
	scores, and all risk scores sent from neighboring factor vertices.

	Following the core message-passing principle of belief propagation, the
	algorithm performs iterative computation between the factor and variable
	vertex sets. The algorithm begins with all variable vertices selecting
	their maximum local score and sending it to all of their neighboring
	factor vertices. Then, all factor vertices filter the risk scores,
	based on when the individuals came into contact. That is, only risk
	scores recorded prior to at least one occurrence that defines the contact
	are retained for subsequent calculation. A weighted transformation that
	accounts for the amount of time passed from when the risk score was
	recorded and the time of running the algorithm is applied to all risk
	scores from a variable vertex. The maximum of these is sent to the other
	variable vertex connected to the factor vertex.

	This completes one iteration of the algorithm and is repeated until
	either a certain number of iterations has passed or the summed difference
	in maximum variable risk scores from the previous iteration drops below a
	set tolerance, whichever condition is satisfied first.

	Attributes:
		transmission_rate: The scaling applied to each weighted risk score at
			the factor vertex. A value of 1 implies that only the effect of
			the exponential time decay weighting scheme affects which risk
			score is sent to the opposite variable vertex.
		tolerance: The minimum value that must be exceeded to continue onto
			the next iteration. The summed difference of all variable
			vertices' max risk scores between the previous iteration and
			current iteration must exceed the tolerance value to continue
			onto the next iteration. The difference is measured, per message
			sent from factors, rather than per variable. Thus, a max risk
			score could be updated multiple times, and all are included in
			the sum.
		iterations: The maximum number of loops the algorithm will run. If
			the tolerance value is not exceeded, then it may not be the case
			that all iterations are completed. For local computing,
			this defines the number of times the variable vertices compute.
			For remote computing, this defines the number of messages a
			variable Ray actor should process before re-calculating the
			tolerance.
		time_buffer: The amount of time (in seconds) that must have
			passed between an occurrence and risk score for it to be retained
			by the factor vertex during computation.
		impl: Implementation to use for the factor graph.
		seed: Random seed to allow for reproducibility. If left unset,
			the current seed is used.
	"""
	transmission_rate = attr.ib(
		type=numbers.Real,
		default=0.8,
		validator=validators.instance_of(numbers.Real),
		converter=float,
		kw_only=True)
	tolerance = attr.ib(
		type=numbers.Real,
		default=1e-6,
		validator=validators.instance_of(numbers.Real),
		converter=float,
		kw_only=True)
	iterations = attr.ib(
		type=numbers.Real,
		default=4,
		validator=validators.instance_of(int),
		kw_only=True)
	time_buffer = attr.ib(
		type=TimestampBuffer,
		default=_TWO_DAYS,
		validator=validators.instance_of((datetime.timedelta, np.timedelta64)),
		converter=lambda x: np.timedelta64(x, 's'),
		kw_only=True)
	time_constant = attr.ib(
		type=numbers.Real,
		default=1,
		validator=validators.instance_of(numbers.Real),
		converter=float,
		kw_only=True)
	msg_threshold = attr.ib(
		type=numbers.Real,
		default=0,
		validator=validators.instance_of(numbers.Real),
		converter=float,
		kw_only=True)
	default_msg = attr.ib(
		type=model.RiskScore,
		default=_DEFAULT_MESSAGE,
		validator=validators.instance_of(model.RiskScore),
		kw_only=True)
	impl = attr.ib(
		type=str,
		default=graphs.DEFAULT,
		validator=validators.in_(graphs.OPTIONS),
		kw_only=True)
	seed = attr.ib(type=Optional[Any], default=None, kw_only=True)

	def __attrs_post_init__(self):
		super(BeliefPropagation, self).__init__()
		if self.seed is not None:
			random.seed(self.seed)
			np.random.seed(self.seed)

	@transmission_rate.validator
	def _check_transmission_rate(self, _, value):
		if value < 0 or float(value) > 1:
			raise ValueError(
				"'transmission_rate' must be between 0 and 1, inclusive")

	@tolerance.validator
	def _check_tolerance(self, _, value):
		if value <= 0:
			raise ValueError(
				f"'tolerance' must be greater than 0; got {value}")

	@iterations.validator
	def _check_iterations(self, _, value):
		if value < 1:
			raise ValueError(f"'iterations' must be at least 1; got {value}")

	@msg_threshold.validator
	def _check_msg_threshold(self, _, value):
		if not 0 <= value <= 1:
			raise ValueError(
				f"'msg_threshold' must be between 0 and 1, inclusive; got "
				f"{value}")


@attr.s(slots=True)
class LocalBeliefPropagation(BeliefPropagation):
	"""A single-process implementation of BeliefPropagation."""
	_graph = attr.ib(type=graphs.FactorGraph, init=False, repr=False)
	_variables = attr.ib(type=stores.VertexStore, init=False, repr=False)
	_factors = attr.ib(type=stores.VertexStore, init=False, repr=False)

	def __attrs_post_init__(self):
		super(LocalBeliefPropagation, self).__attrs_post_init__()

	def __call__(
			self, *, factors: Contacts, variables: AllRiskScores) -> Result:
		stdout('-----------START BELIEF PROPAGATION-----------')
		result = self._call(factors, variables)
		stdout('------------END BELIEF PROPAGATION------------')
		return result

	@codetiming.Timer(text='Total duration: {:0.6f} s', logger=stdout)
	def _call(self, factors: Contacts, variables: AllRiskScores) -> Result:
		self._create_graph(factors, variables)
		i, epoch = 1, None
		stop = False
		while not stop:
			stdout(f'-----------Iteration {i}-----------')
			epoch = self._send_to_factors()
			self._send_to_variables()
			stop = self._stopping_condition(i, epoch)
			i += 1
			stdout(f'---------------------------------')
		return ((v, self._variables.get(v, 'max')) for v in self._variables)

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Creating graph: {:0.6f} s', logger=stdout)
	def _create_graph(
			self, factors: Contacts, variables: AllRiskScores) -> NoReturn:
		builder = graphs.FactorGraphBuilder(
			impl=self.impl,
			share_graph=False,
			graph_as_actor=False,
			use_vertex_store=True,
			num_stores=(1, 1),
			store_as_actor=False,
			detached=False)
		# Must add variables before adding edges
		self._add_variables(builder, variables)
		self._add_factors_and_edges(builder, factors)
		self._graph, _, _, (factors, variables) = builder.build()
		self._factors = factors[0]
		self._variables = variables[0]

	def _add_variables(
			self,
			builder: graphs.FactorGraphBuilder,
			variables: AllRiskScores) -> NoReturn:
		vertices = []
		attrs = {}
		for k, v in ((str(k), v) for k, v in variables):
			vertices.append(k)
			v1, v2 = itertools.tee(v)
			attrs[k] = {
				'max': max(v1, default=self.default_msg),
				'inbox': {k: tuple(v2)}}
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
			attrs[k] = {'occurrences': f.occurrences, 'inbox': {}}
		builder.add_factors(vertices, attributes=attrs)
		builder.add_edges(edges)

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Sending to factors: {:0.6f} s', logger=stdout)
	def _send_to_factors(self) -> np.ndarray:
		def update_max(sender, incoming: Dict):
			curr_max = self._variables.get(sender, 'max')
			# First iteration only: messages only from self
			if sender in incoming:
				values = itertools.chain([curr_max], incoming[sender])
			else:
				values = itertools.chain([curr_max], incoming.values())
			if (updated := max(values)) > curr_max:
				difference = updated.value - curr_max.value
				self._variables.put([sender], {sender: {'max': updated}})
			else:
				difference = 0
			return difference

		def send(sender, incoming: Dict):
			for f in self._graph.get_neighbors(sender):
				# First iteration only: messages only from self
				if sender in incoming:
					from_others = (
						o for o in incoming[sender]
						if o.value > self.msg_threshold)
				else:
					from_others = (
						incoming[o] for o in incoming
						if o != f and incoming[o].value > self.msg_threshold)
				outgoing = {f: {'inbox': {sender: from_others}}}
				self._factors.put([f], outgoing, merge=True)

		epoch = []
		for v in self._variables:
			inbox = self._variables.get(v, 'inbox')
			diff = update_max(v, inbox)
			epoch.append(diff)
			send(v, inbox)
			self._variables.put([v], {v: {'inbox': {}}})
		return np.array(epoch)

	# noinspection PyTypeChecker
	@codetiming.Timer(text='Sending to variables: {:0.6f} s', logger=stdout)
	def _send_to_variables(self) -> NoReturn:
		for f in self._factors:
			neighbors = tuple(self._graph.get_neighbors(f))
			inbox: Dict = self._factors.get(f, 'inbox')
			for i, n in enumerate(neighbors):
				# Assumes factor vertex has a degree of 2
				receiver = neighbors[not i]
				msg = self._compute_message(f, inbox[n])
				outgoing = {receiver: {'inbox': {f: msg}}}
				self._variables.put([receiver], outgoing, merge=True)
			self._factors.put([f], {f: {'inbox': {}}})

	def _compute_message(
			self,
			factor: graphs.Vertex,
			msg: Iterable[model.RiskScore]) -> model.RiskScore:
		def sec_to_day(s: np.ndarray) -> np.float64:
			return np.divide(np.float64(s), 86400)

		occurrences = self._factors.get(factor, attribute='occurrences')
		occurrences = np.array([o.as_array() for o in occurrences]).flatten()
		most_recent = np.max(occurrences['timestamp'])
		msg = np.array([m.as_array() for m in msg]).flatten()
		if msg.size:
			old = np.where(msg['timestamp'] <= most_recent + self.time_buffer)
			msg = msg[old]
		no_valid_messages = not msg.size
		not_transmitted = np.random.uniform() > self.transmission_rate
		if np.any((no_valid_messages, not_transmitted)):
			msg = self.default_msg
		else:
			# Formats time delta as partial days
			diff = sec_to_day(msg['timestamp'] - most_recent)
			# Weight can only decrease original message value
			np.clip(diff, -np.inf, 0, out=diff)
			msg['value'] *= np.exp(diff / self.time_constant)
			msg.sort(order=['value', 'timestamp', 'name'])
			msg = model.RiskScore.from_array(msg[-1])
		return msg

	def _stopping_condition(self, iteration: int, epoch: np.ndarray = None):
		if iteration == 1:
			stop = False
			stdout(f'Epoch tolerance: n/a')
		else:
			diff = sum(epoch)
			stop = diff < self.tolerance or iteration >= self.iterations
			stdout(f'Iteration tolerance: {np.round(diff, 6)}')
		return stop


@attr.s(slots=True)
class RemoteBeliefPropagation(BeliefPropagation):
	pass

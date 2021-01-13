import abc
import contextlib
from typing import Any, Callable, Hashable, Iterable, Mapping, NoReturn, Tuple

import attr
import igraph
import networkx
import numpy as np
import psutil
import ray

NETWORKX = 'networkx'
IGRAPH = 'igraph'
OPTIONS = {NETWORKX, IGRAPH}
NUM_CPUS = psutil.cpu_count(logical=False)
LOGGER: Callable = print


def _array_factory():
	return np.empty((0,))


@attr.s(slots=True)
class FactorGraph(abc.ABC):

	@property
	@abc.abstractmethod
	def factors(self):
		pass

	@factors.setter
	@abc.abstractmethod
	def factors(self, value):
		pass

	@property
	@abc.abstractmethod
	def variables(self):
		pass

	@variables.setter
	@abc.abstractmethod
	def variables(self, value):
		pass

	@abc.abstractmethod
	def get_neighbors(self, vertex):
		pass

	@abc.abstractmethod
	def get_vertex_attr(self, vertex, key):
		pass

	@abc.abstractmethod
	def set_vertex_attr(self, vertex, key, value):
		pass

	@abc.abstractmethod
	def get_edge_attr(self, edge, key):
		pass

	@abc.abstractmethod
	def set_edge_attr(self, edge, key, value):
		pass

	@abc.abstractmethod
	def add_vertices(self, vertices, attributes):
		pass

	@abc.abstractmethod
	def add_edges(self, edges, attributes):
		pass


@attr.s(slots=True)
class IGraphFactorGraph(FactorGraph):
	_graph = attr.ib(type=igraph.Graph, factory=igraph.Graph)
	_factors = attr.ib(type=Iterable[Hashable], factory=_array_factory)
	_variables = attr.ib(type=Iterable[Hashable], factory=_array_factory)

	def __attrs_post_init__(self):
		super(IGraphFactorGraph, self).__init__()

	@property
	def factors(self) -> Iterable[Hashable]:
		return self._factors

	@factors.setter
	def factors(self, value: Iterable[Hashable]) -> NoReturn:
		self._factors = np.array(list(value))

	@property
	def variables(self) -> Iterable[Hashable]:
		return self._variables

	@variables.setter
	def variables(self, value: Iterable[Hashable]) -> NoReturn:
		self._variables = np.array(list(value))

	def get_neighbors(self, vertex: Hashable) -> Iterable[igraph.Vertex]:
		return self._graph.neighbors(vertex)

	def get_vertex_attr(self, vertex: Hashable, key: Hashable) -> Any:
		return self._graph.vs.find(name=vertex).attributes()[key]

	def set_vertex_attr(
			self, vertex: Hashable, key: Hashable, value: Any) -> NoReturn:
		self._graph.vs.find(name=vertex).update_attributes({key: value})

	def get_edge_attr(
			self, edge: Tuple[Hashable, Hashable], key: Hashable) -> Any:
		return self._graph.es[self._graph.get_eid(*edge)].attributes()[key]

	def set_edge_attr(
			self,
			edge: Tuple[Hashable, Hashable],
			key: Hashable,
			value: Any) -> NoReturn:
		edge = self._graph.es[self._graph.get_eid(*edge)]
		edge.update_attributes({key: value})

	def add_vertices(
			self, vertices: Iterable, attributes: Mapping) -> NoReturn:
		for v in vertices:
			self._graph.add_vertex(v, **attributes[v])

	def add_edges(self, edges: Iterable, attributes: Mapping) -> NoReturn:
		for e in edges:
			self._graph.add_edge(*e, **attributes[e])


@attr.s(slots=True)
class NetworkXFactorGraph(FactorGraph):
	_graph = attr.ib(type=networkx.Graph, factory=networkx.Graph)
	_factors = attr.ib(type=Iterable[Hashable], factory=_array_factory)
	_variables = attr.ib(type=Iterable[Hashable], factory=_array_factory)

	def __attrs_post_init__(self):
		super(NetworkXFactorGraph, self).__init__()

	@property
	def factors(self) -> Iterable[Hashable]:
		return self._factors

	@factors.setter
	def factors(self, value: Iterable[Hashable]) -> NoReturn:
		self._factors = np.array(list(value))

	@property
	def variables(self) -> Iterable[Hashable]:
		return self._variables

	@variables.setter
	def variables(self, value: Iterable[Hashable]) -> NoReturn:
		self._variables = np.array(list(value))

	def get_neighbors(self, vertex: Hashable) -> Iterable[Hashable]:
		return self._graph.neighbors(vertex)

	def get_vertex_attr(self, vertex: Hashable, key: Hashable) -> Any:
		return self._graph.nodes[vertex][key]

	def set_vertex_attr(
			self, vertex: Hashable, key: Hashable, value: Any) -> NoReturn:
		self._graph.nodes[vertex][key] = value

	def get_edge_attr(
			self, edge: Tuple[Hashable, Hashable], key: Hashable) -> Any:
		return self._graph.get_edge_data(*edge)[key]

	def set_edge_attr(
			self, edge: Tuple[Hashable, Hashable],
			key: Hashable,
			value: Any) -> NoReturn:
		self._graph[edge[0]][edge[1]][key] = value

	def add_vertices(
			self,
			vertices: Iterable[Hashable],
			attributes: Mapping) -> NoReturn:
		self._graph.add_nodes_from((v, attributes[v]) for v in vertices)

	def add_edges(
			self,
			edges: Iterable[Tuple[Hashable, Hashable]],
			attributes: Mapping) -> NoReturn:
		self._graph.add_edges_from(((*e, attributes[e]) for e in edges))


@contextlib.contextmanager
def ray_context(*args, **kwargs):
	try:
		yield ray.init(*args, **kwargs)
	finally:
		ray.shutdown()

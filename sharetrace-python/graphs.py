import abc
import contextlib
from typing import Any, Hashable, Iterable, Mapping, NoReturn, Tuple

import attr
import igraph
import networkx
import numpy as np
import ray

Edge = Tuple[Hashable, Hashable]
Attributes = Mapping[Hashable, Any]
Vertex = Hashable
NETWORKX = 'networkx'
IGRAPH = 'igraph'
OPTIONS = [NETWORKX, IGRAPH]


def _array_factory():
	return np.empty((0,))


@contextlib.contextmanager
def ray_context(*args, **kwargs):
	try:
		yield ray.init(*args, **kwargs)
	finally:
		ray.shutdown()


@attr.s(slots=True)
class FactorGraph(abc.ABC):

	@abc.abstractmethod
	def get_factors(self):
		pass

	@abc.abstractmethod
	def set_factors(self, value):
		pass

	@abc.abstractmethod
	def get_variables(self):
		pass

	@abc.abstractmethod
	def set_variables(self, value):
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
	_factors = attr.ib(type=Iterable[Vertex], factory=_array_factory)
	_variables = attr.ib(type=Iterable[Vertex], factory=_array_factory)

	def __attrs_post_init__(self):
		super(IGraphFactorGraph, self).__init__()

	def get_factors(self) -> Iterable[Hashable]:
		return self._factors

	def set_factors(self, value: Iterable[Vertex]) -> NoReturn:
		self._factors = np.array(list(value))

	def get_variables(self) -> Iterable[Vertex]:
		return self._variables

	def set_variables(self, value: Iterable[Vertex]) -> NoReturn:
		self._variables = np.array(list(value))

	def get_neighbors(self, vertex: Vertex) -> Iterable[igraph.Vertex]:
		return self._graph.neighbors(vertex)

	def get_vertex_attr(self, vertex: Vertex, key: Hashable) -> Any:
		return self._graph.vs.find(name=vertex).attributes()[key]

	def set_vertex_attr(
			self, vertex: Vertex, key: Hashable, value: Any) -> NoReturn:
		self._graph.vs.find(name=vertex).update_attributes({key: value})

	def get_edge_attr(self, edge: Edge, key: Hashable) -> Any:
		return self._graph.es[self._graph.get_eid(*edge)].attributes()[key]

	def set_edge_attr(self, edge: Edge, key: Hashable, value: Any) -> NoReturn:
		edge = self._graph.es[self._graph.get_eid(*edge)]
		edge.update_attributes({key: value})

	def add_vertices(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes]) -> NoReturn:
		for v in vertices:
			self._graph.add_vertex(v, **attributes[v])

	def add_edges(
			self,
			edges: Iterable[Edge],
			attributes: Mapping[Edge, Attributes]) -> NoReturn:
		for e in edges:
			self._graph.add_edge(*e, **attributes[e])


@attr.s(slots=True)
class NetworkXFactorGraph(FactorGraph):
	_graph = attr.ib(type=networkx.Graph, factory=networkx.Graph)
	_factors = attr.ib(type=Iterable[Vertex], factory=_array_factory)
	_variables = attr.ib(type=Iterable[Vertex], factory=_array_factory)

	def __attrs_post_init__(self):
		super(NetworkXFactorGraph, self).__init__()

	def get_factors(self) -> Iterable[Vertex]:
		return self._factors

	def set_factors(self, value: Iterable[Vertex]) -> NoReturn:
		self._factors = np.array(list(value))

	def get_variables(self) -> Iterable[Vertex]:
		return self._variables

	def set_variables(self, value: Iterable[Vertex]) -> NoReturn:
		self._variables = np.array(list(value))

	def get_neighbors(self, vertex: Vertex) -> Iterable[Vertex]:
		return self._graph.neighbors(vertex)

	def get_vertex_attr(self, vertex: Vertex, key: Hashable) -> Any:
		return self._graph.nodes[vertex][key]

	def set_vertex_attr(
			self, vertex: Vertex, key: Hashable, value: Any) -> NoReturn:
		self._graph.nodes[vertex][key] = value

	def get_edge_attr(self, edge: Edge, key: Hashable) -> Any:
		return self._graph.get_edge_data(*edge)[key]

	def set_edge_attr(self, edge: Edge, key: Hashable, value: Any) -> NoReturn:
		self._graph[edge[0]][edge[1]][key] = value

	def add_vertices(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes]) -> NoReturn:
		self._graph.add_nodes_from((v, attributes[v]) for v in vertices)

	def add_edges(
			self,
			edges: Iterable[Edge],
			attributes: Mapping[Edge, Attributes]) -> NoReturn:
		self._graph.add_edges_from(((*e, attributes[e]) for e in edges))


@ray.remote
@attr.s(slots=True)
class RayFactorGraph(FactorGraph):
	backend = attr.ib(type=str, default=IGRAPH)
	_graph = attr.ib(type=FactorGraph, init=False)

	def __attrs_post_init__(self):
		if self.backend == IGRAPH:
			self._graph = ray.remote(IGraphFactorGraph()).remote()
		elif self.backend == NETWORKX:
			self._graph = ray.remote(NetworkXFactorGraph()).remote()
		else:
			msg = f'Backend must be one of the following: {OPTIONS}'
			raise AttributeError(msg)

	def get_factors(self):
		return ray.get(self._graph.get_factors.remote())

	def set_factors(self, value):
		return ray.get(self._graph.set_factors.remote(value))

	def get_variables(self):
		return ray.get(self._graph.get_variables.remote())

	def set_variables(self, value):
		return ray.get(self._graph.set_variables.remote(value))

	def get_neighbors(self, vertex):
		return ray.get(self._graph.get_neighbors.remote(vertex))

	def get_vertex_attr(self, vertex, key):
		return ray.get(self._graph.get_vertex_attr.remote(vertex, key))

	def set_vertex_attr(self, vertex, key, value):
		return ray.get(self._graph.set_vertex_attr.remote(vertex, key, value))

	def get_edge_attr(self, edge, key):
		return ray.get(self._graph.get_edge_attr.remote(edge, key))

	def set_edge_attr(self, edge, key, value):
		return ray.get(self._graph.set_edge_attr.remote(edge, key, value))

	def add_vertices(self, vertices, attributes):
		return ray.get(self._graph.add_vertices.remote(vertices, attributes))

	def add_edges(self, edges, attributes):
		return ray.get(self._graph.add_eges.remote(edges, attributes))


@attr.s(slots=True)
class DataStore:
	_items = attr.ib(type=np.array, converter=np.array)

	def lookup(self, value):
		return self._items[value]



import abc
from typing import Any, Hashable, Iterable, Mapping, NoReturn, Optional, Tuple

import attr
import igraph
import networkx
import ray

Edge = Tuple[Hashable, Hashable]
Attributes = Mapping[Hashable, Any]
Vertex = Hashable
NETWORKX = 'networkx'
IGRAPH = 'igraph'
OPTIONS = [NETWORKX, IGRAPH]


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
	def add_variables(self, vertices, attributes=None):
		pass

	@abc.abstractmethod
	def add_factors(self, vertices, attributes=None):
		pass

	@abc.abstractmethod
	def add_edges(self, edges, attributes=None):
		pass


@attr.s(slots=True)
class IGraphFactorGraph(FactorGraph):
	_graph = attr.ib(type=igraph.Graph, init=False)
	_factors = attr.ib(type=Iterable[Vertex], init=False)
	_variables = attr.ib(type=Iterable[Vertex], init=False)

	def __attrs_post_init__(self):
		super(IGraphFactorGraph, self).__init__()
		self._graph = igraph.Graph()
		self._factors = set()
		self._variables = set()

	def get_factors(self) -> Iterable[Vertex]:
		return frozenset(self._factors)

	def set_factors(self, value: Iterable[Vertex]) -> NoReturn:
		self._factors = set(value)

	def get_variables(self) -> Iterable[Vertex]:
		return frozenset(self._variables)

	def set_variables(self, value: Iterable[Vertex]) -> NoReturn:
		self._variables = set(value)

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

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		self._add_vertices(vertices, attributes)
		self._variables.update(vertices)

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		self._add_vertices(vertices, attributes)
		self._factors.update(vertices)

	def _add_vertices(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		if attributes is None:
			for v in vertices:
				self._graph.add_vertex(v)
		else:
			for v in vertices:
				self._graph.add_vertex(v, **attributes[v])

	def add_edges(
			self,
			edges: Iterable[Edge],
			attributes: Mapping[Edge, Attributes] = None) -> NoReturn:
		if attributes is None:
			for e in edges:
				self._graph.add_edge(*e)
		else:
			for e in edges:
				self._graph.add_edge(*e, **attributes[e])


@attr.s(slots=True)
class NetworkXFactorGraph(FactorGraph):
	_graph = attr.ib(type=networkx.Graph, init=False)
	_factors = attr.ib(type=Iterable[Vertex], init=False)
	_variables = attr.ib(type=Iterable[Vertex], init=False)

	def __attrs_post_init__(self):
		super(NetworkXFactorGraph, self).__init__()
		self._graph = networkx.Graph()
		self._factors = set()
		self._variables = set()

	def get_factors(self) -> Iterable[Vertex]:
		return frozenset(self._factors)

	def set_factors(self, value: Iterable[Vertex]) -> NoReturn:
		self._factors = set(value)

	def get_variables(self) -> Iterable[Vertex]:
		return frozenset(self._variables)

	def set_variables(self, value: Iterable[Vertex]) -> NoReturn:
		self._variables = set(value)

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

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		self._add_vertices(vertices, attributes)
		self._variables.update(vertices)

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		self._add_vertices(vertices, attributes)
		self._factors.update(vertices)

	def _add_vertices(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		if attributes is None:
			self._graph.add_nodes_from(vertices)
		else:
			self._graph.add_nodes_from((v, attributes[v]) for v in vertices)

	def add_edges(
			self,
			edges: Iterable[Edge],
			attributes: Mapping[Edge, Attributes] = None) -> NoReturn:
		self._graph.add_edges_from(((*e, attributes[e]) for e in edges))


@ray.remote
@attr.s(slots=True)
class RayFactorGraph(FactorGraph):
	backend = attr.ib(type=str, default=IGRAPH)
	_graph = attr.ib(type=FactorGraph, init=False)

	def __attrs_post_init__(self):
		if self.backend == IGRAPH:
			self._graph = ray.remote(IGraphFactorGraph).remote()
		elif self.backend == NETWORKX:
			self._graph = ray.remote(NetworkXFactorGraph).remote()
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

	def add_variables(self, vertices, attributes=None):
		return ray.get(self._graph.add_variables.remote(vertices, attributes))

	def add_factors(self, vertices, attributes=None):
		return ray.get(self._graph.add_factors.remote(vertices, attributes))

	def add_edges(self, edges, attributes=None):
		return ray.get(self._graph.add_eges.remote(edges, attributes))


@attr.s(slots=True, frozen=True)
class Message:
	sender = attr.ib(type=Hashable)
	receiver = attr.ib(type=Hashable)
	content = attr.ib(type=Any)


@attr.s(slots=True)
class _VertexStoreActor:
	_store = attr.ib(type=Mapping[Hashable, Any], init=False)

	def __attrs_post_init__(self):
		self._store = {}

	def get(self, key: Hashable, attribute: Any = None) -> Any:
		if attribute is None:
			value = self._store[key]
			value = key if value is None else value
		else:
			value = self._store[key][attribute]
		return value

	def put(
			self,
			keys: Iterable[Hashable],
			attributes: Mapping[Hashable, Any] = None) -> NoReturn:
		if attributes is None:
			for k in keys:
				self._store[k] = None
		else:
			for k in keys:
				self._store[k] = attributes[k]


@attr.s(slots=True)
class VertexStore:
	_actor = attr.ib(type=_VertexStoreActor, init=False)
	as_actor = attr.ib(type=bool, default=True, converter=bool)

	def __attrs_post_init__(self):
		if self.as_actor:
			self._actor = ray.remote(_VertexStoreActor).remote()
		else:
			self._actor = _VertexStoreActor()

	def get(
			self,
			key: Hashable,
			attribute: Any = None,
			as_ref: bool = False) -> Any:
		if self.as_actor:
			value = self._actor.get.remote(key, attribute)
		else:
			value = self._actor.get(key, attribute)
		return value if as_ref or not self.as_actor else ray.get(value)

	def put(
			self,
			keys: Iterable[Hashable],
			attributes: Mapping[Hashable, Any] = None,
			as_ref: bool = False) -> Optional[ray.ObjectRef]:
		value = None
		if self.as_actor:
			value = self._actor.put.remote(keys, attributes)
		else:
			self._actor.put(keys, attributes)
		return value if as_ref or not self.as_actor else ray.get(value)

import abc
from typing import Any, Hashable, Iterable, Mapping, NoReturn, Optional, Tuple

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
DEFAULT = IGRAPH


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

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		self._add_vertices(vertices, attributes)

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

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		self._add_vertices(vertices, attributes)

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


@attr.s(slots=True)
class RayFactorGraph(FactorGraph):
	backend = attr.ib(type=str, default=DEFAULT)
	_graph = attr.ib(type=FactorGraph, init=False)

	def __attrs_post_init__(self):
		self._graph = factor_graph_factory(backend=self.backend, as_ref=True)

	def get_factors(self) -> Iterable[Vertex]:
		return ray.get(self._graph.get_factors.remote())

	def set_factors(self, value: Iterable[Vertex]) -> NoReturn:
		return ray.get(self._graph.set_factors.remote(value))

	def get_variables(self) -> Iterable[Vertex]:
		return ray.get(self._graph.get_variables.remote())

	def set_variables(self, value: Iterable[Vertex]) -> NoReturn:
		return ray.get(self._graph.set_variables.remote(value))

	def get_neighbors(self, vertex: Vertex) -> Iterable[Vertex]:
		return ray.get(self._graph.get_neighbors.remote(vertex))

	def get_vertex_attr(self, vertex, key):
		return ray.get(self._graph.get_vertex_attr.remote(vertex, key))

	def set_vertex_attr(
			self, vertex: Vertex, key: Hashable, value: Any) -> NoReturn:
		return ray.get(self._graph.set_vertex_attr.remote(vertex, key))

	def get_edge_attr(self, edge: Edge, key: Hashable) -> Any:
		return ray.get(self._graph.get_edge_attr.remote(edge, key))

	def set_edge_attr(self, edge: Edge, key: Hashable, value: Any) -> NoReturn:
		return ray.get(self._graph.set_edge_attr.remote(edge, key, value))

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		return ray.get(self._graph.add_variables.remote(vertices, attributes))

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		return ray.get(self._graph.add_factors.remote(vertices, attributes))

	def add_edges(
			self,
			edges: Iterable[Edge],
			attributes: Mapping[Edge, Attributes] = None) -> NoReturn:
		return ray.get(self._graph.add_eges.remote(edges, attributes))


def factor_graph_factory(backend: str, as_ref: bool = False):
	if backend == IGRAPH:
		if as_ref:
			graph = ray.remote(IGraphFactorGraph).remote()
		else:
			graph = IGraphFactorGraph()
	elif backend == NETWORKX:
		if as_ref:
			graph = ray.remote(NetworkXFactorGraph).remote()
		else:
			graph = NetworkXFactorGraph()
	else:
		raise ValueError(f'Backend must be one of the following: {OPTIONS}')
	return graph


@attr.s(slots=True)
class _VertexStore:
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
	_actor = attr.ib(type=_VertexStore, init=False)
	as_ref = attr.ib(type=bool, default=True, converter=bool)

	def __attrs_post_init__(self):
		if self.as_ref:
			self._actor = ray.remote(_VertexStore).remote()
		else:
			self._actor = _VertexStore()

	def get(
			self,
			key: Hashable,
			attribute: Any = None,
			as_ref: bool = False) -> Any:
		if self.as_ref:
			value = self._actor.get.remote(key, attribute)
		else:
			value = self._actor.get(key, attribute)
		return value if as_ref or not self.as_ref else ray.get(value)

	def put(
			self,
			keys: Iterable[Hashable],
			attributes: Mapping[Hashable, Any] = None,
			as_ref: bool = False) -> Optional[ray.ObjectRef]:
		value = None
		if self.as_ref:
			value = self._actor.put.remote(keys, attributes)
		else:
			self._actor.put(keys, attributes)
		return value if as_ref or not self.as_ref else ray.get(value)


@attr.s(slots=True)
class _FactorGraphBuilder:
	use_vertex_store = attr.ib(type=bool, default=True, converter=bool)
	backend = attr.ib(type=str, default=DEFAULT)
	as_ref = attr.ib(type=bool, default=True)
	_graph = attr.ib(type=FactorGraph, init=False)
	_factors = attr.ib(type=Iterable[Vertex], init=False)
	_variables = attr.ib(type=Iterable[Vertex], init=False)
	_vertex_store = attr.ib(type=VertexStore, init=False)

	def __attrs_post_init__(self):
		self._graph = factor_graph_factory(
			backend=self.backend, as_ref=False)
		if self.use_vertex_store:
			self._vertex_store = VertexStore(as_ref=True)
		self._factors = set()
		self._variables = set()

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		if self.use_vertex_store:
			self._graph.add_variables(vertices)
			self._vertex_store.put(vertices, attributes)
		else:
			self._graph.add_variables(vertices, attributes)
		self._variables.update(vertices)

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		if self.use_vertex_store:
			self._graph.add_factors(vertices)
			self._vertex_store.put(vertices, attributes)
		else:
			self._graph.add_factors(vertices, attributes)
		self._factors.update(vertices)

	def add_edges(
			self,
			edges: Iterable[Edge],
			attributes: Mapping[Edge, Attributes] = None) -> NoReturn:
		self._graph.add_edges(edges, attributes)

	def build(self) -> Tuple[
		FactorGraph,
		Optional[Iterable[Vertex]],
		Optional[Iterable[Vertex]],
		Optional[VertexStore]]:
		if self.as_ref:
			graph = ray.put(self._graph)
			factors = ray.put(np.array(list(self._factors)))
			variables = ray.put(np.array(list(self._variables)))
			if self.use_vertex_store:
				handles = (graph, factors, variables, self._vertex_store)
			else:
				handles = (graph, factors, variables, None)
		else:
			self._graph.set_factors(self._factors)
			self._graph.set_variables(self._variables)
			if self.use_vertex_store:
				handles = (self._graph, self._vertex_store)
			else:
				handles = (self._graph, None)
		return handles


@attr.s(slots=True)
class FactorGraphBuilder:
	use_vertex_store = attr.ib(type=bool, default=True, converter=bool)
	backend = attr.ib(type=str, default=DEFAULT)
	as_ref = attr.ib(type=bool, default=True)
	_actor = attr.ib(type=_FactorGraphBuilder, init=False)

	def __attrs_post_init__(self):
		if self.as_ref:
			self._actor = ray.remote(_FactorGraphBuilder).remote(
				use_vertex_store=self.use_vertex_store,
				backend=self.backend,
				as_ref=self.as_ref)
		else:
			self._actor = _FactorGraphBuilder(
				use_vertex_store=self.use_vertex_store,
				backend=self.backend,
				as_ref=self.as_ref)

	def add_variables(self, vertices, attributes=None) -> Optional[Any]:
		if self.as_ref:
			value = self._actor.add_variables.remote(vertices, attributes)
			value = ray.get(value)
		else:
			self._actor.add_variables(vertices, attributes)
			value = None
		return value

	def add_factors(self, vertices, attributes=None) -> Optional[Any]:
		if self.as_ref:
			value = self._actor.add_factors.remote(vertices, attributes)
			value = ray.get(value)
		else:
			self._actor.add_factors(vertices, attributes)
			value = None
		return value

	def add_edges(self, edges, attributes=None) -> Optional[Any]:
		if self.as_ref:
			value = self._actor.add_edges.remote(edges, attributes)
			value = ray.get(value)
		else:
			self._actor.add_edges(edges, attributes)
			value = None
		return value

	def build(self) -> Tuple[
		FactorGraph,
		Optional[Iterable[Vertex]],
		Optional[Iterable[Vertex]],
		Optional[VertexStore]]:
		if self.as_ref:
			value = ray.get(self._actor.build.remote())
		else:
			value = self._actor.build()
		return value


@attr.s(slots=True, frozen=True)
class Message:
	sender = attr.ib(type=Hashable)
	receiver = attr.ib(type=Hashable)
	content = attr.ib(type=Any)

import abc
from typing import Any, Collection, Hashable, Iterable, Mapping, NoReturn, \
	Optional, Tuple, Union

import igraph
import networkx
import numpy as np
import ray

Edge = Tuple[Hashable, Hashable]
Attributes = Mapping[str, Any]
Vertex = Hashable
NETWORKX = 'networkx'
IGRAPH = 'igraph'
OPTIONS = [NETWORKX, IGRAPH]
DEFAULT = IGRAPH


class FactorGraph(abc.ABC):
	__slots__ = []

	def __init__(self):
		super(FactorGraph, self).__init__()

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


class IGraphFactorGraph(FactorGraph):
	__slots__ = ['_graph', '_factors', '_variables']

	def __init__(self):
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


class NetworkXFactorGraph(FactorGraph):
	__slots__ = ['_graph', '_factors', '_variables']

	def __init__(self):
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
		if attributes is None:
			self._graph.add_edges_from(edges)
		else:
			self._graph.add_edges_from(((*e, attributes[e]) for e in edges))


class RayFactorGraph(FactorGraph):
	__slots__ = ['backend', '_graph']

	def __init__(self, backend: str = DEFAULT):
		super(RayFactorGraph, self).__init__()
		self._graph = _factor_graph_factory(backend=backend, as_actor=True)
		self.backend = backend

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
		return ray.get(self._graph.add_edges.remote(edges, attributes))


def _factor_graph_factory(
		backend: str = DEFAULT, as_actor: bool = False) -> FactorGraph:
	if backend == IGRAPH:
		if as_actor:
			graph = ray.remote(IGraphFactorGraph).remote()
		else:
			graph = IGraphFactorGraph()
	elif backend == NETWORKX:
		if as_actor:
			graph = ray.remote(NetworkXFactorGraph).remote()
		else:
			graph = NetworkXFactorGraph()
	else:
		raise ValueError(f'Backend must be one of the following: {OPTIONS}')
	return graph


def factor_graph_factory(backend: str, as_actor: bool = False) -> FactorGraph:
	if as_actor:
		graph = RayFactorGraph(backend=backend)
	else:
		graph = _factor_graph_factory(backend=backend, as_actor=False)
	return graph


class _VertexStore:
	__slots__ = ['_store']

	def __init__(self):
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
			attributes: Optional[Any] = None) -> NoReturn:
		if attributes is None:
			self._store.update(dict.fromkeys(keys, None))
		elif isinstance(attributes, Mapping):
			for k in keys:
				if k in self._store:
					self._store[k].update(attributes[k])
				else:
					self._store[k] = attributes[k]
		else:
			self._store.update(attributes)


class VertexStore:
	__slots__ = ['as_actor', '_actor']

	def __init__(self, as_actor: bool = True):
		self.as_actor = bool(as_actor)
		if as_actor:
			self._actor = ray.remote(_VertexStore).remote()
		else:
			self._actor = _VertexStore()

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


class _FactorGraphBuilder:
	__slots__ = [
		'backend',
		'share_graph',
		'graph_as_actor',
		'use_vertex_store',
		'vertex_store_as_actor',
		'_graph',
		'_factors',
		'_variables',
		'_vertex_store']

	def __init__(
			self,
			backend: str = DEFAULT,
			share_graph: bool = True,
			graph_as_actor: bool = False,
			use_vertex_store: bool = True,
			vertex_store_as_actor: bool = True):
		self._graph = factor_graph_factory(
			backend=backend, as_actor=graph_as_actor)
		self.backend = backend
		self.graph_as_actor = bool(graph_as_actor)
		self.share_graph = bool(share_graph)
		self.use_vertex_store = bool(use_vertex_store)
		self.vertex_store_as_actor = bool(vertex_store_as_actor)
		if use_vertex_store:
			self._vertex_store = VertexStore(as_actor=vertex_store_as_actor)
		else:
			self._vertex_store = None
		self._factors = set()
		self._variables = set()

	def add_variables(
			self,
			vertices: Collection[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		if self.use_vertex_store:
			self._graph.add_variables(vertices)
			self._vertex_store.put(vertices, attributes)
		else:
			self._graph.add_variables(vertices, attributes)
		self._variables.update(vertices)

	def add_factors(
			self,
			vertices: Collection[Vertex],
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

	def build(self) -> Union[
		Tuple[FactorGraph, Iterable[Vertex], Iterable[Vertex], VertexStore],
		Tuple[FactorGraph, Iterable[Vertex], Iterable[Vertex]],
		Tuple[FactorGraph, VertexStore],
		FactorGraph]:
		if self.share_graph:
			factors = ray.put(np.array(list(self._factors)))
			variables = ray.put(np.array(list(self._variables)))
			graph = ray.put(self._graph)
			if self.use_vertex_store:
				handles = (graph, factors, variables, self._vertex_store)
			else:
				handles = (graph, factors, variables)
		else:
			if self.graph_as_actor:
				if self.use_vertex_store:
					handles = (
						self._graph,
						self._factors,
						self._variables,
						self._vertex_store)
				else:
					handles = (self._graph, self._factors, self._variables)
			else:
				self._graph.set_factors(self._factors)
				self._graph.set_variables(self._variables)
				if self.use_vertex_store:
					handles = (self._graph, self._vertex_store)
				else:
					handles = self._graph
		return handles


class FactorGraphBuilder:
	__slots__ = [
		'backend',
		'as_actor',
		'share_graph',
		'graph_as_actor',
		'use_vertex_store',
		'vertex_store_as_actor',
		'_actor']

	def __init__(
			self,
			backend: str = DEFAULT,
			as_actor: bool = True,
			share_graph: bool = True,
			graph_as_actor: bool = False,
			use_vertex_store: bool = True,
			vertex_store_as_actor: bool = True):
		if as_actor:
			self._actor = ray.remote(_FactorGraphBuilder).remote(
				backend=backend,
				share_graph=share_graph,
				graph_as_actor=graph_as_actor,
				use_vertex_store=use_vertex_store,
				vertex_store_as_actor=vertex_store_as_actor)
		else:
			self._actor = _FactorGraphBuilder(
				backend=backend,
				share_graph=share_graph,
				graph_as_actor=graph_as_actor,
				use_vertex_store=use_vertex_store,
				vertex_store_as_actor=vertex_store_as_actor)
		self.backend = backend
		self.as_actor = bool(as_actor)
		self.share_graph = bool(share_graph)
		self.graph_as_actor = bool(graph_as_actor)
		self.use_vertex_store = bool(use_vertex_store)
		self.vertex_store_as_actor = bool(vertex_store_as_actor)

	def add_variables(self, vertices, attributes=None) -> Optional[Any]:
		if self.as_actor:
			value = self._actor.add_variables.remote(vertices, attributes)
			value = ray.get(value)
		else:
			self._actor.add_variables(vertices, attributes)
			value = None
		return value

	def add_factors(self, vertices, attributes=None) -> Optional[Any]:
		if self.as_actor:
			value = self._actor.add_factors.remote(vertices, attributes)
			value = ray.get(value)
		else:
			self._actor.add_factors(vertices, attributes)
			value = None
		return value

	def add_edges(self, edges, attributes=None) -> Optional[Any]:
		if self.as_actor:
			value = self._actor.add_edges.remote(edges, attributes)
			value = ray.get(value)
		else:
			self._actor.add_edges(edges, attributes)
			value = None
		return value

	def build(self) -> Union[
		Tuple[FactorGraph, Iterable[Vertex], Iterable[Vertex], VertexStore],
		Tuple[FactorGraph, Iterable[Vertex], Iterable[Vertex]],
		Tuple[FactorGraph, VertexStore],
		FactorGraph]:
		if self.as_actor:
			value = ray.get(self._actor.build.remote())
		else:
			value = self._actor.build()
		return value


class Message:
	__slots__ = ['sender', 'receiver', 'content']

	def __init__(self, sender: Hashable, receiver: Hashable, content: Any):
		self.sender = sender
		self.receiver = receiver
		self.content = content

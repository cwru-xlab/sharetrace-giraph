import abc
import collections
import itertools
from typing import (
	Any, Hashable, Iterable, Mapping, NoReturn, Optional, Sequence, Tuple,
	Type, Union)

import igraph
import networkx
import numpy as np
import ray

import backend
import stores

Edge = Tuple[Hashable, Hashable]
Vertex = Hashable
Attributes = Union[Any, Mapping[Hashable, Any]]
VertexAttributes = Mapping[Vertex, Attributes]
EdgeAttributes = Mapping[Edge, Attributes]
VertexVector = Sequence[Tuple[Vertex, int]]
VertexMatrix = Sequence[Sequence[Tuple[Vertex, int]]]
OptionalVertexStores = Optional[Sequence[stores.VertexStore]]
NETWORKX = 'networkx'
IGRAPH = 'igraph'
NUMPY = 'numpy'
OPTIONS = (NETWORKX, IGRAPH, NUMPY)
DEFAULT = NUMPY
_EDGE_ATTRIBUTE_EXCEPTION = '{} does not support edge attributes'
_VERTEX_ATTRIBUTE_EXCEPTION = '{} does not support vertex attributes'
_KILL_EXCEPTION = '{} does not support kill(); use RayFactorGraph instead.'
_IMPL_EXCEPTION = f"'impl' must be one of the following: {OPTIONS}"
_NUM_STORES_CPU_EXCEPTION = (
	"'num_stores' must be less than the number of physical cpus: {}")
_MIN_NUM_STORES_EXCEPTION = "'num_stores' must be at least 1"
_GRAPH_STATE_EXCEPTION = (
	"Only one of 'graph_as_actor' and 'share_graph' can be set to True")


# AttributeError results when using attrs
class FactorGraph(abc.ABC):
	"""An abstract implementation of a factor graph."""
	__slots__ = []

	def __init__(self):
		super(FactorGraph, self).__init__()

	@abc.abstractmethod
	def get_factors(self) -> Iterable[Vertex]:
		pass

	@abc.abstractmethod
	def set_factors(self, value: Iterable[Vertex]) -> bool:
		pass

	@abc.abstractmethod
	def get_variables(self) -> Iterable[Vertex]:
		pass

	@abc.abstractmethod
	def set_variables(self, value: Iterable[Vertex]) -> bool:
		pass

	@abc.abstractmethod
	def get_neighbors(self, vertex: Vertex) -> Iterable[Vertex]:
		pass

	@abc.abstractmethod
	def get_vertex_attr(
			self, vertex: Vertex, *, key: Hashable) -> Iterable[Vertex]:
		pass

	@abc.abstractmethod
	def set_vertex_attr(
			self, vertex: Vertex, *, key: Hashable, value: Any) -> bool:
		pass

	@abc.abstractmethod
	def get_edge_attr(self, edge: Edge, *, key: Hashable) -> Any:
		pass

	@abc.abstractmethod
	def set_edge_attr(self, edge: Edge, *, key: Hashable, value: Any) -> bool:
		pass

	@abc.abstractmethod
	def add_variables(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		pass

	@abc.abstractmethod
	def add_factors(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		pass

	@abc.abstractmethod
	def add_edges(
			self,
			edges: Iterable[Edge],
			*,
			attributes: EdgeAttributes = None) -> bool:
		pass


class IGraphFactorGraph(FactorGraph):
	"""A factor graph implemented using the python-igraph package."""

	__slots__ = ['_graph', '_factors', '_variables']

	def __init__(self):
		super(IGraphFactorGraph, self).__init__()
		self._graph = igraph.Graph()
		self._factors = set()
		self._variables = set()

	def get_factors(self) -> Iterable[Vertex]:
		return frozenset(self._factors)

	def set_factors(self, value: Iterable[Vertex]) -> bool:
		self._factors = set(value)
		return True

	def get_variables(self) -> Iterable[Vertex]:
		return frozenset(self._variables)

	def set_variables(self, value: Iterable[Vertex]) -> bool:
		self._variables = set(value)
		return True

	def get_neighbors(self, vertex: Vertex) -> Iterable[Vertex]:
		neighbors = self._graph.neighbors(vertex)
		if 'name' in self._graph.vs.attribute_names():
			neighbors = (self._graph.vs[n]['name'] for n in neighbors)
		return neighbors

	def get_vertex_attr(
			self, vertex: Vertex, *, key: Hashable) -> Iterable[Vertex]:
		return self._graph.vs.find(name=vertex).attributes()[key]

	def set_vertex_attr(
			self, vertex: Vertex, *, key: Hashable, value: Any) -> bool:
		self._graph.vs.find(name=vertex).update_attributes({key: value})
		return True

	def get_edge_attr(self, edge: Edge, *, key: Hashable) -> Any:
		return self._graph.es[self._graph.get_eid(*edge)][key]

	def set_edge_attr(self, edge: Edge, *, key: Hashable, value: Any) -> bool:
		edge = self._graph.es[self._graph.get_eid(*edge)]
		edge.update_attributes({key: value})
		return True

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		return self._add_vertices(vertices, attributes)

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		return self._add_vertices(vertices, attributes)

	def _add_vertices(
			self,
			vertices: Iterable[Vertex],
			attributes: VertexAttributes = None) -> bool:
		if attributes is None:
			for v in vertices:
				self._graph.add_vertex(v)
		else:
			for v in vertices:
				self._graph.add_vertex(v, **attributes[v])
		return True

	def add_edges(
			self,
			edges: Iterable[Edge],
			*,
			attributes: EdgeAttributes = None) -> bool:
		if attributes is None:
			for e in edges:
				self._graph.add_edge(*e)
		else:
			for e in edges:
				self._graph.add_edge(*e, **attributes[e])
		return True


class NetworkXFactorGraph(FactorGraph):
	"""A factor graph implemented using the networkx package."""

	__slots__ = ['_graph', '_factors', '_variables']

	def __init__(self):
		super(NetworkXFactorGraph, self).__init__()
		self._graph = networkx.Graph()
		self._factors = set()
		self._variables = set()

	def get_factors(self) -> Iterable[Vertex]:
		return frozenset(self._factors)

	def set_factors(self, value: Iterable[Vertex]) -> bool:
		self._factors = set(value)
		return True

	def get_variables(self) -> Iterable[Vertex]:
		return frozenset(self._variables)

	def set_variables(self, value: Iterable[Vertex]) -> bool:
		self._variables = set(value)
		return True

	def get_neighbors(self, vertex: Vertex) -> Iterable[Vertex]:
		return self._graph.neighbors(vertex)

	def get_vertex_attr(
			self, vertex: Vertex, *, key: Hashable) -> Iterable[Vertex]:
		return self._graph.nodes[vertex][key]

	def set_vertex_attr(
			self, vertex: Vertex, *, key: Hashable, value: Any) -> bool:
		self._graph.nodes[vertex][key] = value
		return True

	def get_edge_attr(self, edge: Edge, *, key: Hashable) -> Any:
		return self._graph.get_edge_data(*edge)[key]

	def set_edge_attr(self, edge: Edge, *, key: Hashable, value: Any) -> bool:
		self._graph[edge[0]][edge[1]][key] = value
		return True

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		return self._add_vertices(vertices, attributes)

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		return self._add_vertices(vertices, attributes)

	def _add_vertices(
			self,
			vertices: Iterable[Vertex],
			attributes: VertexAttributes = None) -> bool:
		if attributes is None:
			self._graph.add_nodes_from(vertices)
		else:
			self._graph.add_nodes_from((v, attributes[v]) for v in vertices)
		return True

	def add_edges(
			self,
			edges: Iterable[Edge],
			*,
			attributes: EdgeAttributes = None) -> bool:
		if attributes is None:
			self._graph.add_edges_from(edges)
		else:
			self._graph.add_edges_from(((*e, attributes[e]) for e in edges))
		return True


class NumpyFactorGraph(FactorGraph):
	"""A factor graph implemented using a dictionary and numpy arrays.

	Notes:
		The key is a vertex identifier and the value is a numpy array of the
		neighbors of the vertex.

		Vertex and edge attributes are not supported. Use VertexStore to
		store vertex attributes.
	"""

	__slots__ = ['_graph', '_variables', '_factors']

	def __init__(self):
		super(NumpyFactorGraph, self).__init__()
		self._graph = collections.defaultdict(lambda: np.array([]))
		self._factors = set()
		self._variables = set()

	def get_factors(self) -> Iterable[Vertex]:
		return frozenset(self._factors)

	def set_factors(self, value: Iterable[Vertex]) -> bool:
		self._factors = set(value)
		return True

	def get_variables(self) -> Iterable[Vertex]:
		return frozenset(self._variables)

	def set_variables(self, value: Iterable[Vertex]) -> bool:
		self._variables = set(value)
		return True

	def get_neighbors(self, vertex: Vertex) -> Iterable[Vertex]:
		return self._graph[vertex]

	# noinspection PyTypeChecker
	def get_vertex_attr(
			self,
			vertex: Vertex,
			*,
			key: Hashable) -> Iterable[Vertex]:
		cls = self.__class__.__name__
		raise NotImplementedError(_VERTEX_ATTRIBUTE_EXCEPTION.format(cls))

	# noinspection PyTypeChecker
	def set_vertex_attr(
			self,
			vertex: Vertex,
			*,
			key: Hashable,
			value: Any) -> bool:
		cls = self.__class__.__name__
		raise NotImplementedError(_VERTEX_ATTRIBUTE_EXCEPTION.format(cls))

	def get_edge_attr(self, edge: Edge, *, key: Hashable) -> Any:
		cls = self.__class__.__name__
		raise NotImplementedError(_EDGE_ATTRIBUTE_EXCEPTION.format(cls))

	# noinspection PyTypeChecker
	def set_edge_attr(self, edge: Edge, *, key: Hashable, value: Any) -> bool:
		cls = self.__class__.__name__
		raise NotImplementedError(_EDGE_ATTRIBUTE_EXCEPTION.format(cls))

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		return self._add_vertices(vertices, attributes, variables=True)

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		return self._add_vertices(vertices, attributes, variables=False)

	def _add_vertices(
			self,
			vertices: Iterable[Vertex],
			attributes: VertexAttributes = None,
			variables: bool = True) -> NoReturn:
		if attributes is not None:
			cls = self.__class__.__name__
			raise NotImplementedError(_VERTEX_ATTRIBUTE_EXCEPTION.format(cls))
		if variables:
			self._variables.update(vertices)
		else:
			self._factors.update(vertices)
		self._graph.fromkeys(vertices, np.array([]))
		return True

	def add_edges(
			self,
			edges: Iterable[Edge],
			*,
			attributes: EdgeAttributes = None) -> bool:
		if attributes is not None:
			cls = self.__class__.__name__
			raise NotImplementedError(_EDGE_ATTRIBUTE_EXCEPTION.format(cls))
		for (v1, v2) in edges:
			self._graph[v1] = np.append(self._graph[v1], np.array([v2]))
			self._graph[v2] = np.append(self._graph[v2], np.array([v1]))
		return True


class RayFactorGraph(FactorGraph, backend.ActorMixin):
	__slots__ = ['detached', '_actor']

	def __init__(self, graph: Type, *, detached: bool = False):
		super(RayFactorGraph, self).__init__()
		self._actor = ray.remote(graph)
		if detached:
			self._actor = self._actor.options(lifetime='detached')
		self._actor = self._actor.remote()

	def get_factors(self) -> Iterable[Vertex]:
		return ray.get(self._actor.get_factors.remote())

	def set_factors(self, value: Iterable[Vertex]) -> bool:
		return ray.get(self._actor.set_factors.remote(value))

	def get_variables(self) -> Iterable[Vertex]:
		return ray.get(self._actor.get_variables.remote())

	def set_variables(self, value: Iterable[Vertex]) -> bool:
		return ray.get(self._actor.set_variables.remote(value))

	def get_neighbors(self, vertex: Vertex) -> Iterable[Vertex]:
		return ray.get(self._actor.get_neighbors.remote(vertex))

	def get_vertex_attr(
			self, vertex: Vertex, *, key: Hashable) -> Iterable[Vertex]:
		return ray.get(self._actor.get_vertex_attr.remote(vertex, key=key))

	def set_vertex_attr(
			self,
			vertex: Vertex,
			*, key: Hashable,
			value: Any) -> bool:
		func = self._actor.set_vertex_attr
		return ray.get(func.remote(vertex, key=key, value=value))

	def get_edge_attr(self, edge: Edge, *, key: Hashable) -> Any:
		return ray.get(self._actor.get_edge_attr.remote(edge, key=key))

	def set_edge_attr(self, edge: Edge, *, key: Hashable, value: Any) -> bool:
		func = self._actor.set_edge_attr
		return ray.get(func.remote(edge, key=key, value=value))

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		func = self._actor.add_variables
		return ray.get(func.remote(vertices, attributes=attributes))

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		func = self._actor.add_factors
		return ray.get(func.remote(vertices, attributes=attributes))

	def add_edges(
			self,
			edges: Iterable[Edge],
			*,
			attributes: EdgeAttributes = None) -> bool:
		func = self._actor.add_edges
		return ray.get(func.remote(edges, attributes=attributes))

	def kill(self) -> NoReturn:
		ray.kill(self._actor)


class FactorGraphBuilder:
	"""Builder for instantiating FactorGraph implementations.

	Attributes:
		impl: Package implementation of the factor graph.
		share_graph: True pins the FactorGraph in shared object store memory
			using the ray package.
		graph_as_actor: True instantiates the graph as an actor using the ray
			package.
		use_vertex_store: True stores vertex attributes in a VertexStore.
		num_stores: If greater than 1, vertices are round-robin assigned to
			multiple vertex stores.
		store_as_actor: True instantiates the VertexStore as an actor using
			the ray package.
		detached: True instantiates the FactorGraph and VertexStore as
			detached actors, using the ray package. Has no effect of the
			corresponding flag parameter for using as an actor is set to False.
	"""

	__slots__ = [
		'impl',
		'share_graph',
		'graph_as_actor',
		'use_vertex_store',
		'num_stores',
		'store_as_actor',
		'detached',
		'_graph',
		'_variables',
		'_factors',
		'_stores',
		'_store_ind']

	def __init__(
			self,
			impl: str = DEFAULT,
			*,
			share_graph: bool = False,
			graph_as_actor: bool = False,
			use_vertex_store: bool = False,
			num_stores: int = 1,
			store_as_actor: bool = False,
			detached: bool = False):
		self.impl = str(impl)
		self.share_graph = bool(share_graph)
		self.graph_as_actor = bool(graph_as_actor)
		self.use_vertex_store = bool(use_vertex_store),
		self.num_stores = int(num_stores)
		self.store_as_actor = bool(store_as_actor)
		self.detached = bool(detached)
		self._cross_check_graph_state()
		self._graph = factor_graph_factory(
			impl=impl, as_actor=graph_as_actor, detached=detached)
		if self.use_vertex_store:
			self._cross_check_num_stores()
			self._stores = tuple(
				stores.VertexStore(
					local_mode=not store_as_actor,
					detached=detached)
				for _ in range(self.num_stores))
		else:
			self._stores = None
		self._store_ind = 0
		n = 1 if self.num_stores == 0 else self.num_stores
		self._factors = [[] for _ in range(n)]
		self._variables = [[] for _ in range(n)]

	def _cross_check_num_stores(self):
		if self.use_vertex_store:
			num_cpus = backend.NUM_CPUS
			if self.store_as_actor and self.num_stores > num_cpus:
				raise ValueError(_NUM_STORES_CPU_EXCEPTION.format(num_cpus))
			if self.num_stores < 1:
				raise ValueError(_MIN_NUM_STORES_EXCEPTION)

	def _cross_check_graph_state(self):
		if self.graph_as_actor and self.share_graph:
			raise ValueError(_GRAPH_STATE_EXCEPTION)

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		return self._add_vertices(
			vertices,
			attributes,
			self._graph.add_variables,
			self._variables)

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		return self._add_vertices(
			vertices,
			attributes,
			self._graph.add_factors,
			self._factors)

	def _add_vertices(self, vertices, attrs, add_func, all_vertices) -> bool:
		if self.use_vertex_store:
			if self.num_stores > 1:
				for v in vertices:
					add_func([v])
					ind = self._store_ind
					all_vertices[ind].append((v, ind))
					self._stores[ind].put(keys=[v], attributes={v: attrs[v]})
					self._increment()
			else:
				self._stores[0].put(keys=vertices, attributes=attrs)
				all_vertices[0].extend((v, 0) for v in vertices)
		else:
			add_func(vertices, attrs)
		return True

	def add_edges(
			self,
			edges: Iterable[Edge],
			*,
			attributes: EdgeAttributes = None) -> NoReturn:
		self._graph.add_edges(edges, attributes=attributes)

	def build(self, *, set_graph: bool = False) -> Tuple[
		FactorGraph,
		Union[VertexVector, VertexMatrix],
		Union[VertexVector, VertexMatrix],
		OptionalVertexStores]:
		"""Instantiates the built FactorGraph and optional VertexStore.

		If multiple VertexStores are used, the factor and variable vertex
		lists are list of lists, each list corresponding to the VertexStore
		in which its attributes are stored.

		Args:
			set_graph: True will store the factor and variable lists in the
				Factor Graph instance.

		Returns:
			FactorGraph instance, factor and variable lists, and optional
			VertexStore instance.
		"""
		graph = self._graph
		factors = self._factors
		variables = self._variables
		if set_graph:
			if self.num_stores > 1:
				v_ids = itertools.chain.from_iterable(variables)
				v_ids = (v for v, _ in v_ids)
				f_ids = itertools.chain.from_iterable(factors)
				f_ids = (v for v, _ in f_ids)
			else:
				v_ids = variables[0]
				f_ids = factors[0]
			graph.set_variables(v_ids)
			graph.set_factors(f_ids)
			factors = None
			variables = None
		if self.share_graph:
			graph = ray.put(graph)
		return graph, factors, variables, self._stores

	def _increment(self):
		reset = (plus_one := self._store_ind + 1) >= self.num_stores
		self._store_ind = 0 if reset else plus_one


class FGPart(abc.ABC, backend.ActorMixin):
	__slots__ = []

	def __init__(self):
		super(FGPart, self).__init__()

	@abc.abstractmethod
	def send_to_factors(self, *args, **kwargs) -> bool:
		pass

	@abc.abstractmethod
	def send_to_variables(self, *args, **kwargs) -> bool:
		pass


def factor_graph_factory(
		impl: str = DEFAULT,
		*,
		as_actor: bool = False,
		detached: bool = False) -> FactorGraph:
	if impl == IGRAPH:
		graph = IGraphFactorGraph
	elif impl == NETWORKX:
		graph = NetworkXFactorGraph
	elif impl == NUMPY:
		graph = NumpyFactorGraph
	else:
		raise ValueError(_IMPL_EXCEPTION)
	if as_actor:
		graph = RayFactorGraph(graph, detached=detached)
	else:
		graph = graph()
	return graph

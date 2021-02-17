import abc
import collections
import functools
import itertools
from typing import (
	Any, Collection, Hashable, Iterable, Mapping, NoReturn, Optional, Sequence,
	Tuple, Type, Union)

import igraph
import networkx
import numpy as np
import ray

import backend
import stores

# Type aliases
Vertex = Hashable
Edge = Tuple[Vertex, Vertex]
Attributes = Union[Hashable, Mapping[Hashable, Any]]
Vertices = Union[Iterable[Vertex], Mapping[Vertex, Attributes]]
Edges = Union[Iterable[Edge], Mapping[Edge, Attributes]]
VertexVector = Sequence[Tuple[Vertex, int]]
VertexMatrix = Sequence[Sequence[Tuple[Vertex, int]]]
VertexStores = Sequence[stores.VertexStore]
OptionalVertexStores = Optional[
	Union[Tuple[VertexStores, VertexStores], VertexStores]]
# Implementation options
NETWORKX = 'networkx'
IGRAPH = 'igraph'
NUMPY = 'numpy'
OPTIONS = (NETWORKX, IGRAPH, NUMPY)
DEFAULT = NUMPY
# Exception messages
_EDGE_ATTRIBUTE_EXCEPTION = '{} does not support edge attributes'


# AttributeError results when using attrs
class FactorGraph(abc.ABC):
	"""An abstract implementation of a factor graph."""
	__slots__ = []

	def __init__(self):
		super(FactorGraph, self).__init__()

	def __repr__(self):
		cls = self.__class__.__name__
		return backend.rep(cls)

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
	def add_variables(self, vertices):
		pass

	@abc.abstractmethod
	def add_factors(self, vertices):
		pass

	@abc.abstractmethod
	def add_edges(self, edges):
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

	def set_factors(self, value: Iterable[Vertex]) -> NoReturn:
		self._factors = set(value)

	def get_variables(self) -> Iterable[Vertex]:
		return frozenset(self._variables)

	def set_variables(self, value: Iterable[Vertex]) -> NoReturn:
		self._variables = set(value)

	def get_neighbors(self, vertex: Vertex) -> Iterable[Vertex]:
		neighbors = self._graph.neighbors(vertex)
		if 'name' in self._graph.vs.attribute_names():
			neighbors = (self._graph.vs[n]['name'] for n in neighbors)
		return neighbors

	def get_vertex_attr(self, vertex: Vertex, key: Hashable) -> Any:
		return self._graph.vs.find(name=vertex).attributes()[key]

	def set_vertex_attr(
			self, vertex: Vertex, key: Hashable, value) -> NoReturn:
		self._graph.vs.find(name=vertex).update_attributes({key: value})

	def get_edge_attr(self, edge: Edge, key: Hashable) -> Any:
		return self._graph.es[self._graph.get_eid(*edge)][key]

	def set_edge_attr(self, edge: Edge, key: Hashable, value) -> NoReturn:
		edge = self._graph.es[self._graph.get_eid(*edge)]
		edge.update_attributes({key: value})

	def add_variables(self, vertices: Vertices) -> NoReturn:
		self._add_vertices(vertices)

	def add_factors(self, vertices: Vertices) -> NoReturn:
		self._add_vertices(vertices)

	def _add_vertices(self, vertices: Vertices) -> NoReturn:
		add_vertex = self._graph.add_vertex
		if isinstance(vertices, Mapping):
			for v in vertices:
				add_vertex(v, **vertices[v])
		else:
			for v in vertices:
				add_vertex(v)

	def add_edges(self, edges: Edges) -> NoReturn:
		add_edge = self._graph.add_edge
		if isinstance(edges, Mapping):
			for e in edges:
				add_edge(*e, **edges[e])
		else:
			self._graph.add_edges(edges)


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
			self, vertex: Vertex, key: Hashable, value) -> NoReturn:
		self._graph.nodes[vertex][key] = value

	def get_edge_attr(self, edge: Edge, key: Hashable) -> Any:
		return self._graph.get_edge_data(*edge)[key]

	def set_edge_attr(self, edge: Edge, key: Hashable, value) -> NoReturn:
		self._graph[edge[0]][edge[1]][key] = value

	def add_variables(self, vertices: Vertices) -> NoReturn:
		self._add_vertices(vertices)

	def add_factors(self, vertices: Vertices) -> NoReturn:
		self._add_vertices(vertices)

	def _add_vertices(self, vertices: Vertices) -> NoReturn:
		if isinstance(vertices, Mapping):
			self._graph.add_nodes_from((v, vertices[v]) for v in vertices)
		else:
			self._graph.add_nodes_from(vertices)

	def add_edges(self, edges: Edges) -> NoReturn:
		if isinstance(edges, Mapping):
			self._graph.add_edges_from((e, edges[e]) for e in edges)
		else:
			self._graph.add_edges_from(edges)


class NumpyFactorGraph(FactorGraph):
	"""A factor graph implemented using a dictionary and numpy arrays.

	Notes:
		The key is a vertex identifier and the value is a numpy array of the
		neighbors of the vertex.
	"""

	__slots__ = ['_graph', '_variables', '_factors', '_attrs']

	def __init__(self):
		super(NumpyFactorGraph, self).__init__()
		self._graph = collections.defaultdict(lambda: np.array([]))
		self._attrs = {}
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
		return self._graph[vertex]

	def get_vertex_attr(self, vertex: Vertex, key: Hashable) -> Any:
		return self._attrs[vertex][key]

	def set_vertex_attr(
			self, vertex: Vertex, key: Hashable, value) -> NoReturn:
		self._attrs[vertex][key] = value

	def get_edge_attr(self, edge: Edge, key: Hashable) -> Any:
		return self._attrs[edge][key]

	def set_edge_attr(self, edge: Edge, key: Hashable, value) -> NoReturn:
		self._attrs[edge][key] = value

	def add_variables(self, vertices: Vertices) -> NoReturn:
		self._add_vertices(vertices)

	def add_factors(self, vertices: Vertices) -> NoReturn:
		self._add_vertices(vertices, variables=False)

	def _add_vertices(
			self, vertices: Vertices, variables: bool = True) -> NoReturn:
		if isinstance(vertices, Mapping):
			self._attrs.update(vertices)
		if variables:
			self._variables.update(vertices)
		else:
			self._factors.update(vertices)
		new = (v for v in vertices if v not in self._graph)
		self._graph.fromkeys(new, np.array([]))

	def add_edges(self, edges: Edges) -> NoReturn:
		if isinstance(edges, Mapping):
			self._attrs.update(edges)
		append = np.append
		array = np.array
		graph = self._graph
		for (v1, v2) in edges:
			graph[v1] = append(graph[v1], array([v2]))
			graph[v2] = append(graph[v2], array([v1]))


class RayFactorGraph(FactorGraph, backend.Process):
	__slots__ = ['detached', '_actor']

	def __init__(self, graph: Type, *, detached: bool = False):
		super(RayFactorGraph, self).__init__()
		self._actor = ray.remote(graph)
		if detached:
			self._actor = self._actor.options(lifetime='detached')
		self._actor = self._actor.remote()

	def get_factors(self) -> Iterable[Vertex]:
		return ray.get(self._actor.get_factors.remote())

	def set_factors(self, value: Iterable[Vertex]) -> NoReturn:
		ray.get(self._actor.set_factors.remote(value))

	def get_variables(self) -> Iterable[Vertex]:
		return ray.get(self._actor.get_variables.remote())

	def set_variables(self, value: Iterable[Vertex]) -> NoReturn:
		ray.get(self._actor.set_variables.remote(value))

	def get_neighbors(self, vertex: Vertex) -> Iterable[Vertex]:
		return ray.get(self._actor.get_neighbors.remote(vertex))

	def get_vertex_attr(self, vertex: Vertex, key: Hashable) -> Any:
		return ray.get(self._actor.get_vertex_attr.remote(vertex, key))

	def set_vertex_attr(
			self, vertex: Vertex, key: Hashable, value) -> NoReturn:
		ray.get(self._actor.set_vertex_attr.remote(vertex, key, value))

	def get_edge_attr(self, edge: Edge, key: Hashable) -> Any:
		return ray.get(self._actor.get_edge_attr.remote(edge, key))

	def set_edge_attr(self, edge: Edge, key: Hashable, value) -> NoReturn:
		ray.get(self._actor.set_edge_attr.remote(edge, key, value))

	def add_variables(self, vertices: Vertices) -> NoReturn:
		ray.get(self._actor.add_variables.remote(vertices))

	def add_factors(self, vertices: Vertices) -> NoReturn:
		ray.get(self._actor.add_factors.remote(vertices))

	def add_edges(self, edges: Edges) -> NoReturn:
		ray.get(self._actor.add_edges.remote(edges))

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
		num_stores: If an integer and greater than 1, vertices are assigned
			to each store in a round-robin fashion. If a Sequence of length 2,
			interpreted as (num_factor_stores, num_variable_stores);
			vertices are still assigned in a round-robin fashion, but only
			amongst the stores for the corresponding type. Only non-empty
			stores are retained upon return.
		store_in_graph: A collection of attributes that should be stored in
			the graph as vertex attributes. If using VertexStores, these will
			not be stored in them. 'address' is a reserved attribute
			indicating the VertexStore index in which a vertex's attributes
			are stored. If specified in `store_in_graph`, it will only be
			added to the graph as a vertex attribute. Otherwise, it will be
			automatically added as an attribute in the VertexStore.
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
		'store_in_graph',
		'store_as_actor',
		'detached',
		'_graph',
		'_variables',
		'_factors',
		'_stores',
		'_store_ind',
		'_store_address']

	def __init__(
			self,
			impl: str = DEFAULT,
			*,
			share_graph: bool = False,
			graph_as_actor: bool = False,
			use_vertex_store: bool = False,
			store_in_graph: Collection[str] = None,
			num_stores: Union[int, Tuple[int, int]] = 1,
			store_as_actor: bool = False,
			detached: bool = False):
		self._cross_check_graph_state(graph_as_actor, share_graph)
		self.impl = str(impl)
		self.share_graph = bool(share_graph)
		self.graph_as_actor = bool(graph_as_actor)
		self._graph = factor_graph_factory(
			impl, as_actor=graph_as_actor, detached=detached)
		self.use_vertex_store = bool(use_vertex_store)
		if store_in_graph is None:
			self.store_in_graph = None
			self._store_address = False
		else:
			self.store_in_graph = frozenset(store_in_graph)
			self._store_address = 'address' in store_in_graph
			if self._store_address:
				self.store_in_graph -= {'address'}
		self.store_as_actor = bool(store_as_actor)
		self.detached = bool(detached)
		if self.use_vertex_store:
			self._check_num_stores(num_stores, store_as_actor)
			vertex_store = functools.partial(
				stores.VertexStore,
				local_mode=not store_as_actor,
				detached=detached)
			if isinstance(num_stores, int):
				self.num_stores = int(num_stores)
				self._stores = tuple(
					vertex_store() for _ in range(self.num_stores))
				self._store_ind = 0
			else:
				self.num_stores = tuple(num_stores)
				self._stores = (
					tuple(vertex_store() for _ in range(self.num_stores[0])),
					tuple(vertex_store() for _ in range(self.num_stores[1])))
				self._store_ind = [0, 0]
		else:
			self.num_stores = 0
			self._stores = None
		if isinstance(self.num_stores, int):
			n = min(1, self.num_stores)
			self._factors = [[] for _ in range(n)]
			self._variables = [[] for _ in range(n)]
		else:
			self._factors = [[] for _ in range(self.num_stores[0])]
			self._variables = [[] for _ in range(self.num_stores[1])]

	@staticmethod
	def _check_num_stores(num_stores, store_as_actor):
		def check_num(n):
			if n < 1:
				raise ValueError("int 'num_stores' must be at least 1")

		if not isinstance(num_stores, (int, Tuple)):
			raise TypeError("'num_stores' must be an int or Tuple of 2 ints")
		if isinstance(num_stores, int):
			check_num(num_stores)
		if isinstance(num_stores, Tuple):
			if len(num_stores) != 2:
				raise ValueError("Tuple 'num_stores' must be of length 2")
			for x in num_stores:
				check_num(x)
		num_cpus = backend.NUM_CPUS
		if store_as_actor:
			if isinstance(num_stores, int) and num_stores > num_cpus:
				raise ValueError(
					"'num_stores' must be less than the number of physical "
					f"CPUs: {num_cpus}")
			else:
				total_exceeded = sum(num_stores) > num_cpus
				either_exceeded = any(n for n in num_stores if n > num_cpus)
				if total_exceeded or either_exceeded:
					raise ValueError(
						"Neither the sum, nor individual entry in "
						"'num_stores' must exceed the number of physical "
						f"CPUs; got {num_stores}")

	@staticmethod
	def _cross_check_graph_state(graph_as_actor, share_graph):
		if graph_as_actor and share_graph:
			raise ValueError(
				"Only one of 'graph_as_actor' and 'share_graph' can True")

	def __repr__(self):
		return backend.rep(
			self.__class__.__name__,
			share_graph=self.share_graph,
			graph_as_actor=self.graph_as_actor,
			use_vertex_store=self.use_vertex_store,
			store_in_graph=self.store_in_graph,
			num_stores=self.num_stores,
			store_as_actor=self.store_as_actor,
			detached=self.detached)

	def add_variables(self, vertices: Vertices) -> NoReturn:
		self._add_vertices(
			to_add=vertices,
			add=self._graph.add_variables,
			added=self._variables,
			variables=True)

	def add_factors(self, vertices: Vertices) -> NoReturn:
		self._add_vertices(
			to_add=vertices,
			add=self._graph.add_factors,
			added=self._factors,
			variables=False)

	def _add_vertices(self, to_add, add, added, variables) -> NoReturn:
		if self.use_vertex_store:
			self._add_with_store(to_add, add, added, variables)
		else:
			add(to_add)

	def _add_with_store(self, to_add, add, added, variables):
		def get_index() -> int:
			if isinstance(self.num_stores, int):
				index = self._store_ind
			else:
				index = self._store_ind[variables]
			self._increment(variables)
			return index

		def get_store(index: int) -> stores.VertexStore:
			if isinstance(self.num_stores, int):
				store = self._stores[index]
			else:
				store = self._stores[variables][index]
			return store

		store_in_graph = self.store_in_graph
		if store_in_graph:
			in_graph, in_store = {}, {}
			for v, a in ((v, a) for v in to_add for a in to_add[v]):
				if a in store_in_graph:
					in_graph[v][a] = to_add[v][a]
				else:
					in_store[v][a] = to_add[v][a]
		else:
			in_graph, in_store = collections.defaultdict(dict), to_add

		storage = in_graph if self._store_address else in_store
		for v in to_add:
			i = get_index()
			added[i].append(v)
			storage[v]['address'] = i
			add({v: in_graph[v]})
			get_store(i).put({v: in_store[v]})

	def add_edges(self, edges: Edges) -> NoReturn:
		self._graph.add_edges(edges)

	def build(self, *, set_graph: bool = False) -> Tuple[
		FactorGraph,
		Union[VertexVector, VertexMatrix],
		Union[VertexVector, VertexMatrix],
		OptionalVertexStores]:
		"""Instantiates the built FactorGraph and optional VertexStore.

		If multiple VertexStores are used, then the factor and variable vertex
		lists are each a list of lists, each list corresponding to the
		VertexStore in which its attributes are stored.

		Args:
			set_graph: True will store the factor and variable lists in the
				Factor Graph instance.

		Returns:
			FactorGraph instance, factor and variable lists, and optional
			VertexStore instances.
		"""
		graph = self._graph
		# Remove empty partitions and stores
		factors = (f for f in self._factors if f)
		variables = (v for v in self._variables if v)
		if self.use_vertex_store:
			if isinstance(self.num_stores, int):
				vertex_stores = tuple(s for s in self._stores if s)
			else:
				vertex_stores = tuple(
					tuple(s for s in group if s) for group in self._stores)
		else:
			vertex_stores = None
		if set_graph:
			v_ids = (v for v, _ in itertools.chain.from_iterable(variables))
			f_ids = (f for f, _ in itertools.chain.from_iterable(factors))
			graph.set_variables(v_ids)
			graph.set_factors(f_ids)
			factors, variables = None, None
		else:
			factors = tuple(factors)
			variables = tuple(variables)
		if self.share_graph:
			graph = ray.put(graph)
		return graph, factors, variables, vertex_stores

	def _increment(self, variables: bool = True):
		# noinspection PyTypeChecker
		def int_increment():
			reset = (plus_one := self._store_ind + 1) >= self.num_stores
			self._store_ind = 0 if reset else plus_one

		# noinspection PyUnresolvedReferences
		def sequence_increment(i):
			reset = (plus_one := self._store_ind[i] + 1) >= self.num_stores[i]
			self._store_ind[i] = 0 if reset else plus_one

		if isinstance(self._store_ind, int):
			int_increment()
		else:
			sequence_increment(variables)


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
		raise ValueError(f"'impl' must be one of the following: {OPTIONS}")
	return RayFactorGraph(graph, detached=detached) if as_actor else graph()

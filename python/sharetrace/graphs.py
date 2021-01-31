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

Edge = Tuple[Hashable, Hashable]
Vertex = Hashable
Attributes = Union[Any, Mapping[Hashable, Any]]
VertexAttributes = Mapping[Vertex, Attributes]
EdgeAttributes = Mapping[Edge, Attributes]
VertexVector = Sequence[Tuple[Vertex, int]]
VertexMatrix = Sequence[Sequence[Tuple[Vertex, int]]]
VertexStores = Sequence[stores.VertexStore]
OptionalVertexStores = Optional[
	Union[Tuple[VertexStores, VertexStores], VertexStores]]
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

	def __repr__(self):
		cls = self.__class__.__name__
		return backend.rep(cls)

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
	def get_vertex_attr(self, vertex: Vertex, *, key: Hashable) -> Any:
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

	def get_vertex_attr(self, vertex: Vertex, *, key: Hashable) -> Any:
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

	def get_vertex_attr(self, vertex: Vertex, *, key: Hashable) -> Any:
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

		Edge attributes are not supported.
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
	def get_vertex_attr(self, vertex: Vertex, *, key: Hashable) -> Any:
		return self._attrs[vertex][key]

	# noinspection PyTypeChecker
	def set_vertex_attr(
			self,
			vertex: Vertex,
			*,
			key: Hashable,
			value: Any) -> bool:
		self._attrs[vertex][key] = value
		return True

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
			for v in attributes:
				self._attrs[v] = attributes[v]
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

	def get_vertex_attr(self, vertex: Vertex, *, key: Hashable) -> Any:
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
			num_stores: Union[int, Sequence] = 1,
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
				self.store_in_graph = self.store_in_graph - {'address'}
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

		if not isinstance(num_stores, (int, Sequence)):
			raise TypeError(
				"'num_stores' must be an int or Sequence of 2 ints")
		if isinstance(num_stores, int):
			check_num(num_stores)
		if isinstance(num_stores, Sequence):
			if len(num_stores) != 2:
				raise ValueError("Sequence 'num_stores' must be of length 2")
			for x in num_stores:
				check_num(x)
		num_cpus = backend.NUM_CPUS
		if store_as_actor:
			if isinstance(num_stores, int) and num_stores > num_cpus:
				raise ValueError(_NUM_STORES_CPU_EXCEPTION.format(num_cpus))
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
			raise ValueError(_GRAPH_STATE_EXCEPTION)

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

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		return self._add_vertices(
			to_add=vertices,
			attrs=attributes,
			add_func=self._graph.add_variables,
			added=self._variables,
			variables=True)

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			*,
			attributes: VertexAttributes = None) -> bool:
		return self._add_vertices(
			to_add=vertices,
			attrs=attributes,
			add_func=self._graph.add_factors,
			added=self._factors,
			variables=False)

	def _add_vertices(self, to_add, attrs, add_func, added, variables) -> bool:
		if self.use_vertex_store:
			self._add_with_store(to_add, attrs, add_func, added, variables)
		else:
			add_func(to_add, attrs)
		return True

	def _add_with_store(self, to_add, attrs, add_func, added, variables):
		if self.store_in_graph:
			in_graph = functools.partial(lambda a: a in self.store_in_graph)
			in_store = functools.partial(lambda a: not in_graph(a))
			in_graph = {
				v: {a: attrs[v][a] for a in attrs[v] if in_graph(a)}
				for v in attrs}
			in_store = {
				v: {a: attrs[v][a] for a in attrs[v] if in_store(a)}
				for v in attrs}
		else:
			in_graph, in_store = collections.defaultdict(dict), attrs

		def common_add(index, vertex):
			added[index].append(vertex)
			if self._store_address:
				in_graph[vertex].update({'address': index})
				add_func([vertex], attributes={vertex: in_graph[vertex]})
			else:
				add_func([vertex], attributes=in_graph)

		def int_add():
			for v in to_add:
				ind = self._store_ind
				common_add(ind, v)
				if not self._store_address:
					in_store[v].update({'address': ind})
					attributes = {v: in_store[v]}
				else:
					attributes = in_store
				self._stores[ind].put(keys=[v], attributes=attributes)
				self._increment()

		def seq_add(i):
			for v in to_add:
				ind = self._store_ind[i]
				common_add(ind, v)
				if not self._store_address:
					in_store[v]['address'] = ind
					attributes = {v: in_store[v]}
				else:
					attributes = in_store
				self._stores[i][ind].put(keys=[v], attributes=attributes)
				self._increment(variables)

		int_add() if isinstance(self.num_stores, int) else seq_add(variables)

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
		def int_incr():
			reset = (plus_one := self._store_ind + 1) >= self.num_stores
			self._store_ind = 0 if reset else plus_one

		def seq_incr(i):
			reset = (plus_one := self._store_ind[i] + 1) >= self.num_stores[i]
			self._store_ind[i] = 0 if reset else plus_one

		int_incr() if isinstance(self._store_ind, int) else seq_incr(variables)


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

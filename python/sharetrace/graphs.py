import abc
import collections
import itertools
from typing import (
	Any, Hashable, Iterable, Mapping, NoReturn, Optional, Sequence, Tuple,
	Union)

import attr
import igraph
import networkx
import numpy as np
import ray

import backend
from stores import VertexStore

Edge = Tuple[Hashable, Hashable]
Attributes = Mapping[str, Any]
Vertex = Hashable
VertexVector = Sequence[Tuple[Vertex, int]]
VertexMatrix = Sequence[Sequence[Tuple[Vertex, int]]]
OptionalVertexStores = Optional[Sequence[VertexStore]]
NETWORKX = 'networkx'
IGRAPH = 'igraph'
NUMPY = 'numpy'
OPTIONS = (NETWORKX, IGRAPH, NUMPY)
DEFAULT = NUMPY
_EDGE_ATTRIBUTE_EXCEPTION = '{} does not support edge attributes'
_VERTEX_ATTRIBUTE_EXCEPTION = '{} does not support vertex attributes'
_KILL_EXCEPTION = '{} does not support kill() as it is not a Ray Actor'
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

	@abc.abstractmethod
	def kill(self):
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

	def get_neighbors(
			self, vertex: Vertex) -> Union[Iterable[str], Iterable[int]]:
		neighbors = self._graph.neighbors(vertex)
		if 'name' in self._graph.vs.attribute_names():
			neighbors = (self._graph.vs[n]['name'] for n in neighbors)
		return neighbors

	def get_vertex_attr(self, vertex: Vertex, key: Hashable) -> Any:
		return self._graph.vs.find(name=vertex).attributes()[key]

	def set_vertex_attr(
			self, vertex: Vertex, key: Hashable, value: Any) -> NoReturn:
		self._graph.vs.find(name=vertex).update_attributes({key: value})

	def get_edge_attr(self, edge: Edge, key: Hashable) -> Any:
		return self._graph.es[self._graph.get_eid(*edge)][key]

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

	def kill(self) -> NoReturn:
		cls = self.__class__.__name__
		raise NotImplementedError(_KILL_EXCEPTION.format(cls))


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

	def kill(self) -> NoReturn:
		cls = self.__class__.__name__
		raise NotImplementedError(_KILL_EXCEPTION.format(cls))


class NumpyFactorGraph(FactorGraph):
	"""A factor graph implemented using a dictionary and numpy arrays.

	Notes:
		The key is a vertex identifier and the value is a numpy array of the
		neighbors of the vertex.

		Vertex and edge attributes are not supported. Use the VertexStore to
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

	def set_factors(self, value: Iterable[Vertex]) -> NoReturn:
		self._factors = set(value)

	def get_variables(self) -> Iterable[Vertex]:
		return frozenset(self._variables)

	def set_variables(self, value: Iterable[Vertex]) -> NoReturn:
		self._variables = set(value)

	def get_neighbors(self, vertex: Vertex) -> Iterable[Vertex]:
		return self._graph[vertex]

	def get_vertex_attr(self, vertex: Vertex, key: Hashable) -> Any:
		cls = self.__class__.__name__
		raise NotImplementedError(_VERTEX_ATTRIBUTE_EXCEPTION.format(cls))

	def set_vertex_attr(
			self, vertex: Vertex, key: Hashable, value: Any) -> NoReturn:
		cls = self.__class__.__name__
		raise NotImplementedError(_VERTEX_ATTRIBUTE_EXCEPTION.format(cls))

	def get_edge_attr(self, edge: Edge, key: Hashable) -> Any:
		cls = self.__class__.__name__
		raise NotImplementedError(_EDGE_ATTRIBUTE_EXCEPTION.format(cls))

	def set_edge_attr(self, edge: Edge, key: Hashable, value: Any) -> NoReturn:
		cls = self.__class__.__name__
		raise NotImplementedError(_EDGE_ATTRIBUTE_EXCEPTION.format(cls))

	def add_variables(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		self._add_vertices(vertices, attributes, variables=True)

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		self._add_vertices(vertices, attributes, variables=False)

	def _add_vertices(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None,
			variables: bool = True) -> NoReturn:
		if attributes is not None:
			cls = self.__class__.__name__
			raise NotImplementedError(_VERTEX_ATTRIBUTE_EXCEPTION.format(cls))
		if variables:
			self._variables.update(vertices)
		else:
			self._factors.update(vertices)
		self._graph.fromkeys(vertices, np.array([]))

	def add_edges(
			self,
			edges: Iterable[Edge],
			attributes: Mapping[Edge, Attributes] = None) -> NoReturn:
		if attributes is not None:
			cls = self.__class__.__name__
			raise NotImplementedError(_EDGE_ATTRIBUTE_EXCEPTION.format(cls))
		for (v1, v2) in edges:
			self._graph[v1] = np.append(self._graph[v1], np.array([v2]))
			self._graph[v2] = np.append(self._graph[v2], np.array([v1]))

	def kill(self) -> NoReturn:
		cls = self.__class__.__name__
		raise NotImplementedError(_KILL_EXCEPTION.format(cls))


# noinspection PyUnresolvedReferences
class RayFactorGraph(FactorGraph):
	__slots__ = ['impl', '_graph']

	def __init__(self, *, impl: str = DEFAULT, detached: bool = True):
		super(RayFactorGraph, self).__init__()
		self._graph = _factor_graph_factory(
			impl=impl, as_actor=True, detached=detached)
		self.impl = impl
		self.detached = detached

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

	def kill(self) -> NoReturn:
		pass


@attr.s(slots=True)
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
		detatched: True instantiates the FactorGraph and VertexStore as
			detached actors, using the ray package. Has no effect of the
			corresponding flag parameter for using as an actor is set to False.
	"""
	impl = attr.ib(type=str, default=DEFAULT)
	share_graph = attr.ib(type=bool, default=True, converter=bool)
	graph_as_actor = attr.ib(type=bool, default=False, converter=bool)
	use_vertex_store = attr.ib(type=bool, default=True, converter=bool)
	num_stores = attr.ib(type=int, default=1, converter=int)
	store_as_actor = attr.ib(type=bool, default=True, converter=bool)
	detached = attr.ib(type=bool, default=True, converter=bool)
	_graph = attr.ib(type=FactorGraph, init=False, repr=False)
	_factors = attr.ib(type=Iterable[Vertex], init=False, repr=False)
	_variables = attr.ib(type=Iterable[Vertex], init=False, repr=False)
	_stores = attr.ib(
		type=Union[VertexStore, Sequence[VertexStore]], init=False, repr=False)
	_store_ind = attr.ib(type=int, init=False, repr=False)

	def __attrs_post_init__(self):
		self._cross_check_graph_state()
		self._graph = factor_graph_factory(
			impl=self.impl,
			as_actor=self.graph_as_actor,
			detached=self.detached)
		if self.use_vertex_store:
			self._cross_check_num_stores()
			self._stores = tuple(
				VertexStore(
					local_mode=not self.store_as_actor,
					detached=self.detached)
				for _ in range(self.num_stores))
			self._store_ind = 0
		else:
			self._stores = None
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
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		self._add_vertices(
			vertices,
			attributes,
			self._graph.add_variables,
			self._variables)

	def add_factors(
			self,
			vertices: Iterable[Vertex],
			attributes: Mapping[Vertex, Attributes] = None) -> NoReturn:
		self._add_vertices(
			vertices,
			attributes,
			self._graph.add_factors,
			self._factors)

	def _add_vertices(self, vertices, attributes, add_func, all_vertices):
		if self.use_vertex_store:
			if self.num_stores > 1:
				for i, v in enumerate(vertices):
					add_func([v])
					ind = self._store_ind
					all_vertices[ind].append((v, ind))
					attrs = {v: attributes[v]}
					self._stores[ind].put(keys=[v], attributes=attrs)
					self._increment()
			else:
				self._stores[0].put(keys=vertices, attributes=attributes)
				all_vertices[0].extend((v, 0) for v in vertices)
		else:
			add_func(vertices, attributes)

	def add_edges(
			self,
			edges: Iterable[Edge],
			attributes: Mapping[Edge, Attributes] = None) -> NoReturn:
		self._graph.add_edges(edges, attributes)

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


def _factor_graph_factory(
		impl: str, as_actor: bool, detached: bool) -> FactorGraph:
	if as_actor:
		if impl == IGRAPH:
			if detached:
				graph = ray.remote(IGraphFactorGraph)
				graph = graph.options(lifetime='detached')
			else:
				graph = ray.remote(IGraphFactorGraph)
		elif impl == NETWORKX:
			if detached:
				graph = ray.remote(NetworkXFactorGraph)
				graph = graph.options(lifetime='detached')
			else:
				graph = ray.remote(IGraphFactorGraph)
		elif impl == NUMPY:
			if detached:
				graph = ray.remote(NumpyFactorGraph)
				graph = graph.options(lifetime='detached')
			else:
				graph = ray.remote(NumpyFactorGraph)
		else:
			raise ValueError(_IMPL_EXCEPTION)
		graph = graph.remote()
	else:
		if impl == IGRAPH:
			graph = IGraphFactorGraph()
		elif impl == NETWORKX:
			graph = NetworkXFactorGraph()
		elif impl == NUMPY:
			graph = NumpyFactorGraph()
		else:
			raise ValueError(_IMPL_EXCEPTION)
	return graph


def factor_graph_factory(
		*,
		impl: str = DEFAULT,
		as_actor: bool = False,
		detached: bool = True) -> FactorGraph:
	if as_actor:
		graph = RayFactorGraph(impl=impl, detached=detached)
	else:
		graph = _factor_graph_factory(
			impl=impl, as_actor=as_actor, detached=detached)
	return graph

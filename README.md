# ShareTrace Computing API

This is the computing backend API for the ShareTrace mobile application. More
information regarding the mobile application can be found on the
[website](https://www.sharetrace.org/) and on the
[community GitHub repo](https://github.com/SafeTrace-community).

## Repo Organization

There are currently two main directories, one for the original Java
implementation and another for the more recent Python implementation. As of this
update to the README (February 3, 2021), the Java implementation is not intended
for use and is not up-to-date with the changes that have been made in the Python
implementation. It does, however, serve as reference for the original
inspiration for what is implemented in the Python version. In fact, much of what
is implemented in the Python implementation draws directly from the Java
version. In particular, the contact searching algorithm was adapted for the
Python syntax, but otherwise remains identical in logic. Additionally, many of
the classes in the `model.py` module were reimplemented from their original Java
classes. Most significantly, the belief propagation implementation in
`propagation.py`, while using the Ray library, relies heavily on the design
concepts that are present in the Java implementation, which used the Apache
Giraph library. Notably, the `RemoteBeliefPropagation` class is quite similar in
design, though does not enforce the synchronization component that is present in
the Apache Giraph computing framework.

## High-Level Design

The following is the high-level process that holds for both the Java and Python
APIs.

1. Local risk scores and timestamped geohash data are collected from all user
   personal data accounts (PDAs) periodically.
2. A mapping to internal Python objects is done for each type of data.
3. To determine who came in contact with each other, an intermediate algorithm (
   contact search) is run using the timestamped geohashes.
4. With the local risk scores and contacts, the main algorithm (belief
   propagation) is run to derive updated risk scores of all users.
5. Updated risk scores are written back to all user PDAs.

## Overview of the Python API

Given that the Java API is currently not being maintained, only details
regarding the Python API will be provided. However, many of the high-level
details still hold. The following is a module-level breakdown.

### `app.py`

Contains the "main" program, currently implemented for use by AWS Lambda. This
module provides an implementation of the high-level design described above.

### `model.py`

Contains all the data objects used in the API, such as `RiskScore`,
`Contact`, and `LocationHistory`. Please refer to the module and classes for
further documentation.

### `pda.py`

Contains the `PdaContext` class used to perform asynchronous communication with
contracted PDAs. Note, at this time, it is written with ShareTrace in mind. For
a more general PDA client that is capable of performing CRUD API calls to PDAs,
please see the sharetrace-pda-common, sharetrace-pda-read, and
sharetrace-pda-write directories in the Java API.

### `search.py`

Contains the `ContactSearch` class that searches for `Contact`s among pairs
of `LocationHistory` objects. Please see the module for the extensive
documentation regarding its implementation.

### `graphs.py`

Contains the `FactorGraph` abstract base class and all concrete implementations
using several graph library (and custom) implementations. Currently, the
supported implementations are networkx, igraph, numpy, and Ray. The numpy
implementation simply uses a dictionary to index the vertices, and a numpy array
to store the neighbors of each vertex. The Ray implementation is a "meta"
implementation in that it can use all other implementations, but wraps them as
an actor (see [their documentation](https://ray.io/) for more details).

In addition to the factor graph implementations, the `FactorGraphBuilder` is a
convenience class for specifying several details when constructing factor graphs
and is the recommended way to instantiate the factor graph implementations.

### `stores.py`

Contains the `Queue` abstract base class and concrete implementations.
Currently, the supported implementations are a local queue (wraps
`collections.deque`), async queue (wraps `asyncio.Queue`) and remote queue
(wraps `ray.util.queue.Queue`). A function factory can be used to instantiate
any of these with the specification of a couple of function parameters.

In addition, the `VertexStore` class is used to store vertices and their
optional attributes. The motivation for this class was to utilize the shared
object memory store present in the Ray library, which allows processes to access
the same object. Thus, it is possible to store the graph in the object store by
separating the stateful (the attributes) for the stateless (the structure).
Under the hood, the `VertexStore` is simply a dictionary, but provides two
methods, `get()` and `put()` that provide for several configurations of what is
stored. Please see the class docstring for more details.

### `propagation.py`

Contains the belief propagation implementation. Note that, as of this time,
the `BeliefPropagation` abstract base class is targeted specifically for the
ShareTrace API, as opposed to generic belief propagation setups. There are two
implementations: `LocalBeliefPropagation` and `RemoteBeliefPropagation`. The
former is intended for single-process use and is capable of handling up to 1000
users. Otherwise, the latter should be used because of its utilization of the
Ray multi-processing library. Please see the docstrings for more details.

### `backend.py`

Contains utility globals and functions.

## Building (Java only)

Note that the following instructions assumes IntelliJ is being used for the
developer's IDE. This project is built with Gradle.

1. Clone the repo.
2. Create a new project in IntelliJ (from existing sources).
3. Select the folder that contains all the repo contents.
4. From "Import project from external model," select Gradle.

To generate source code:

1. Under Settings/Preferences > Build, Execution, Deployment > Compiler >
   Annotation Processor , enable annotation processing.
2. Select "Obtain processors from project classpath."
3. Select "Module content root" for "Store generated sources relative to" and
   specify the desired directories to store the generated code.
4. Under Build, run "Build Project."
4. Under Project Structure > Modules > sharetrace-model > main, mark the
   directory specified as a generated source.

## TODO
- Implement optimizations mentioned in https://towardsdatascience.com/optimizing-your-python-code-156d4b8f4a29
- Extend the `BeliefPropagation` abstract class to outline the general
  framework, e.g. `to_factor_condition()` which decides if a message should be
  sent to a factor vertex. It may be possible to make `call()` a concrete method
  and all others abstract. The challenge is with such large deviations in design
  that can be seen between `LocalBeliefPropagation` and
  `RemoteBeliefPropagation`. This may be solvable via further abstraction.
- Consider moving non-encrypted environment variables to S3, particularly
  parameters pertaining to the network model and the contact-searching
  algorithm.
- Implement a more dynamic heuristic for determining the number of CPUs to
  assign for variable and factor graph partition processes.

# ShareTrace API

## Announcments
Please refer to sharetrace-python for the most up to date API. All other packages will eventually be refactored to use Jython.

## Building
Note that the following instructions assumes IntelliJ is being used for the developer's IDE. This
 project is built with Gradle.
 1. Clone the repo.
 2. Create a new project in IntelliJ (from existing sources).
 3. Select the folder that contains all of the repo contents.
 4. From "Import project from externel model," select Gradle.
 
 To generate source code:
 1. Under Settings/Preferences > Build, Execution, Deployment > Compiler > Annotation Processor
 , enable annotation processing.
 2. Select "Obtain processors from project classpath."
 3. Select "Module content root" for "Store generated sources relative to" and specify the desired directories to store the generated code.
 4. Under Build, run "Build Project."
 4. Under Project Structure > Modules > sharetrace-model > main, mark the directory specified as a generated source.

## TODO
- Extend the `BeliefPropagation` abstract class to outline the general 
  framework, e.g. `to_factor_condition()` which decides if a message should 
  be sent to a factor vertex. It may be possible to make `call()` a concrete
  method and all others abstract. The challenge is with such large 
  deviations in design that can be seen between `LocalBeliefPropagation` and 
  `RemoteBeliefPropagation`. This may be solvable via further abstraction.
- Consider moving non-encrypted environment variables to S3, particularly 
  parameters pertaining to the network model and the contact-searching 
  algorithm.
- Implement a more dynamic heuristic for determining the number of CPUs to 
  assign for variable and factor graph partition processes.
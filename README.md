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

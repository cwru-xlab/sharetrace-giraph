# ShareTrace API

## Repo Overview
This is a location to add resources (i.e., design documentation, notes, external 
links, code) related to the development of the ShareTrace contact tracing 
application.

## API 

The API contains the primary elements that comprise contact tracing:

* `ShareTraceServer`
* `Contact`
* `ContactHistory`
* `UserPDA`

The ShareTraceServer performs risk score calculations, interaction graph
operations, and performs CRUD operations with the UserPDAs.

A Contact is simply a user-to-user interaction, with the Bluetooth IDs being
used to represent each user.

A ContactHistory is a dictionary with the key-value mapping of date-to-IdGroup.
The resolution of the date is left for further implementation. A IdGroup is
a generic collection of users that allows for assignment checking, adding, and
removing.

A UserPDA contains the information about a user that is relevant to ShareTrace's
contact tracing capabilities, such as diagnosis, symptoms, access token,
Bluetooth ID, and contact history.

See the sharetrace_api.py for docstrings for each class as well.
import requests
from abc import ABCMeta, abstractmethod
import json
from enum import Enum
import datetime
import typing
from collections.abc import MutableMapping, KeysView, ValuesView, ItemsView
'''
TODO:
    - Enable asynchronous capabilities (https://docs.python.org/3/library/asyncio.html) 
    - Ask if JSON encoded as strings in the HAT API need to have escape characters.
    - Don't think this is necessary for the API to do (shouldn't the PDA be 
        responsible for this notification?): Provide where the risk score is 
        written to the user, so that the user can pull it from that location of 
        its PDA.
    - Make as much of the library immutable types as possible to avoid unwanted
        mutation from function call or state change.

References:
    https://api.hubofallthings.com/?version=latest#9d8e2b51-04fb-4750-8b09-a49b3d60bc5e
    https://en.wikipedia.org/wiki/List_of_HTTP_header_fields
    https://en.wikipedia.org/wiki/List_of_HTTP_status_codes
    https://requests.readthedocs.io/en/master/
    https://docs.python.org/3/library/index.html
'''

AccessToken = typing.NewType('AccessToken', str)


class ShareTraceServer(metaclass=ABCMeta):

    def __init__(self, client_id: AnyStr):
        self.client_id = client_id
        self.connect()

    @abstractmethod
    def connect(self, **kwargs) -> typing.Any:
        pass

    @abstractmethod
    def disconnect(self, **kwargs) -> typing.Any:
        pass

    @abstractmethod
    def compute(self, **kwargs) -> typing.Any:
        pass

    def response_raise_for_status(self,
                                  request_func: function,
                                  **kwargs) -> requests.Response:
        def requests_function_not_found(func, package_or_module):
            return func not in package_or_module.__dict__.values()

        if requests_function_not_found(request_func, requests):
            message = f'{request_func} is not a {requests.__name__} function.'
            raise NotImplementedError(message).with_traceback()
        response = request_func(kwargs)
        response.raise_for_status()
        return response

    @abstractmethod
    def authenticate_pda(self,
                         url: AnyStr,
                         username: AnyStr,
                         password: AnyStr) -> AccessToken:
        '''
        Additional implementation logic may be required by the API, but the
            basic operations are provided here:

            headers = {
                'Accept': 'application/json',
                'username': username,
                'password': password
            }
            response = response_raise_for_status(requests.get,
                                                url=url,
                                                headers=headers)
            response = response.text.encode('utf8')
            try:
                return response['accessToken']
            else:
                raise KeyError('No access token was found.').with_traceback()
        '''

    @abstractmethod
    def get_from_pda(self,
                     access_token: AnyStr,
                     url: AnyStr,
                     **kwargs) -> typing.Any:
        '''
        Use this to get UserPDA data related to contact tracing.

        From HAT API:
            Accessing the data for an endPoint is easy. HAT limits the response 
                to 1000 items per request. Some extra helpful parameters are:

                    orderBy: the field to use in order to sort the data
                    ordering: descending or ascending
                    take: take n items (maximum number is 1000)
                    skip: skip n items. This can be bigger than 1000

            Additional implementation logic maybe added, but the following is 
                the basic GET request:

                headers = {
                    'Content-Type': 'application/json',
                    'x-auth-token': access_token
                }
                response = response_raise_for_status(requests.get,
                                                     url=url,
                                                     headers=headers)
                response = response.text.encode('utf8')
        '''
        pass

    @abstractmethod
    def post_to_pda(self, access_token: AnyStr, **kwargs) -> typing.Any:
        '''
        May not need, depending on the API behavior.

        See get_from_pda() docstring, except use requests.post.
        '''
        pass

    @abstractmethod
    def put_to_pda(self, access_token: AnyStr, **kwargs) -> typing.Any:
        '''
        Use to update a UserPDA with new contact-tracing-related data.

        See get_from_pda() docstring, except use requests.put.
        '''
        pass

    @abstractmethod
    def del_from_pda(self, access_token: AnyStr, **kwargs) -> typing.Any:
        '''
        May not need, depending on the API behavior.

        See get_from_pda() docstring, except use requests.delete.
        '''
        pass

    def __enter__(self):
        return self

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        '''
        Handle exceptions and connection clean up.
        May also consider implementing asynchronous __aenter__() and 
            __aexit()__ instead.
        '''

        pass

    def __repr__(self):
        return f'{__class__.__name__}(client_id={self.client_id})'


class Contact:

    def __init__(self, user_1: typing.AnyStr, user_2: typing.AnyStr):
        self.user_1 = user_1
        self.user_2 = user_2

    def __repr__(self):
        return f'{__class__.__name__}(user_1={self.user_1}, user_2={self.user_2})'


Date = typing.TypeVar('Date', datetime.datetime, datetime.date)


class ContactHistory(MutableMapping, KeysView, ValuesView, ItemsView):
    '''
    ContactHistory is treated the same as a typical dict.
    '''

    def __init__(self, history: Dict = None):
        if history is None:
            self.history = dict()
        else:
            self.history = history

    def __getitem__(self, key: Date):
        return self.history.__getitem__(key)

    def __setitem__(self, key: Date, value: Contact):
        if not isinstance(key, (datetime.datetime, datetime.date)):
            msg = 'Only keys must be either datetime.datetime or datetime.date.'
            raise TypeError(msg).with_traceback()
        if not isinstance(value, Contact):
            msg = 'Only values must be a Contact.'
            raise TypeError(msg).with_traceback()
        self.history.__setitem__(key, value)

    def __delitem__(self, key):
        self.history.__delitem__(key)

    def __iter__(self):
        return self.history.__iter__()

    def __len__(self):
        return self.history.__len__()

    def keys(self):
        return self.history.keys()

    def values(self):
        return self.history.values()

    def items(self):
        return self.history.items()


class UserPDA(ABCMeta):
    '''
    A PDA belonging to a typical user of ShareTrace. Contains all of the
        expected attributes mentioned in the ShareTrace white paper.
    '''

    def __init__(self,
                 access_token: AnyStr,
                 symptoms: Set = None,
                 diagnosed: bool = False,
                 contact_history: ContactHistory = None):
        self.access_token = access_token
        self.diagnosed = diagnosed
        if symptoms is None:
            self.symptoms = set()
        else:
            self.symptoms = symptoms
        if contact_history is None:
            self.contact_history = ContactHistory()
        else:
            self.contact_history = contact_history

    @abstractmethod
    def generate_bluetooth_id(self, **kwargs):
        pass

from functools import partial, update_wrapper
import json

from klein import Klein

from sqlalchemy import MetaData, Table, Column
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateTable

from twisted.internet.defer import maybeDeferred
from twisted.python import log


class APIError(Exception):
    code = 500

    def __init__(self, message, code=None):
        super(APIError, self).__init__(message)
        if code is not None:
            self.code = code


class BadRequestParams(APIError):
    code = 400


class Service(object):
    """Namespace class for RESTful service decorators.

    This is used by applying the :meth:`service` decorator to a class and then
    applying the :meth:`handler` decorator to handler methods on that class.

    TODO: Document this better.
    """

    @staticmethod
    def handler(*args, **kw):
        """Decorator for HTTP request handlers.

        This decorator takes the same parameters as Klein's ``route()``
        decorator.

        When used on a method of a class decorated with :meth:`service`, the
        method is turned into a Klein request handler and response formatting
        is added.

        TODO: Document this better.
        """
        def deco(func):
            func._handler_args = (args, kw)
            return func
        return deco

    @classmethod
    def service(cls, service_class):
        """Decorator for RESTful API classes.

        This decorator adds a bunch of magic to a class with
        :meth:`handler`-decorated methods on it so it can be used as a Klein
        HTTP resource.

        TODO: Document this better.
        """
        service_class.app = Klein()
        for attr in dir(service_class):
            meth = getattr(service_class, attr)
            if hasattr(meth, '_handler_args'):
                handler = cls._make_handler(service_class, meth)
                setattr(service_class, attr, handler)
        return service_class

    @classmethod
    def _make_handler(cls, service_class, handler_method):
        args, kw = handler_method._handler_args
        wrapper = partial(cls._handler_wrapper, handler_method)
        update_wrapper(wrapper, handler_method)
        route = service_class.app.route(*args, **kw)
        return route(wrapper)

    @classmethod
    def _handler_wrapper(cls, func, self, request, *args, **kw):
        d = maybeDeferred(func, self, request, *args, **kw)
        d.addCallback(cls._format_response, request)
        d.addErrback(self.handle_api_error, request)
        d.addErrback(cls._handle_api_error, request)
        return d

    @staticmethod
    def _handle_api_error(failure, request):
        error = failure.value
        if not failure.check(APIError):
            log.err(failure)
            error = APIError('Internal server error.')
        return Service._format_error(request, error)

    @staticmethod
    def set_request_id(request, request_id):
        # We name-mangle the attr because `request` isn't our object.
        request.__request_id = request_id

    @staticmethod
    def get_request_id(request):
        try:
            return request.__request_id
        except AttributeError:
            return None

    @staticmethod
    def get_params(params, mandatory, optional):
        missing = set(mandatory) - set(params.keys())
        extra = set(params.keys()) - (set(mandatory) | set(optional))
        if missing:
            raise BadRequestParams("Missing request parameters: '%s'" % (
                "', '".join(sorted(missing))))
        if extra:
            raise BadRequestParams("Unexpected request parameters: '%s'" % (
                "', '".join(sorted(extra))))
        return params

    @classmethod
    def _format_response(cls, params, request):
        request.setHeader('Content-Type', 'application/json')
        params['request_id'] = cls.get_request_id(request)
        return json.dumps(params)

    @classmethod
    def _format_error(cls, request, error):
        request.setHeader('Content-Type', 'application/json')
        request.setResponseCode(error.code)
        return json.dumps({
            'request_id': cls.get_request_id(request),
            'error': error.message,
        })


class TableMissingError(Exception):
    pass


class PrefixedTableCollection(object):
    class make_table(object):
        def __init__(self, *args, **kw):
            self.args = args
            self.kw = kw

        def make_table(self, name, metadata):
            return Table(name, metadata, *self.copy_args(), **self.kw)

        def copy_args(self):
            for arg in self.args:
                if isinstance(arg, Column):
                    yield arg.copy()
                else:
                    yield arg

    def get_table_name(self, name):
        return '%s_%s' % (self.name, name)

    def __init__(self, name, connection):
        self.name = name
        self._conn = connection
        self._metadata = MetaData()
        for attr in dir(self):
            attrval = getattr(self, attr)
            if isinstance(attrval, PrefixedTableCollection.make_table):
                setattr(self, attr, attrval.make_table(
                    self.get_table_name(attr), self._metadata))

    def _create_table(self, trx, table):
        # This works around alchimia's current inability to create tables only
        # if they don't already exist.

        def table_exists_errback(f):
            f.trap(OperationalError)
            if 'table %s already exists' % (table.name,) in str(f.value):
                return None
            return f

        d = self._conn.execute(CreateTable(table))
        d.addErrback(table_exists_errback)
        return d.addCallback(lambda r: trx)

    def create_tables(self):
        d = self._conn.begin()
        for table in self._metadata.sorted_tables:
            d.addCallback(self._create_table, table)
        return d.addCallback(lambda trx: trx.commit())

    def exists(self):
        # It would be nice to make this not use private things.
        return self._conn._engine.has_table(
            self._metadata.sorted_tables[0].name)

    def execute_query(self, query, *args, **kw):
        def table_missing_errback(f):
            f.trap(OperationalError)
            if 'no such table: ' in str(f.value):
                raise TableMissingError(f.value.message)
            return f

        d = self._conn.execute(query, *args, **kw)
        return d.addErrback(table_missing_errback)

    def execute_fetchall(self, query, *args, **kw):
        d = self.execute_query(query, *args, **kw)
        return d.addCallback(lambda result: result.fetchall())

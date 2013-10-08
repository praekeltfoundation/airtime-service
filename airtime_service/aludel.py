from functools import partial, update_wrapper
import json

from klein import Klein

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
    @staticmethod
    def handler(*args, **kw):
        def deco(func):
            func._handler_args = (args, kw)
            return func
        return deco

    @classmethod
    def service(cls, service_class):
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

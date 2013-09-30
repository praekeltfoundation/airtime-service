from functools import wraps
import json

from klein import Klein

from twisted.internet.defer import inlineCallbacks, returnValue

from airtime_service.models import (
    get_engine, VoucherPool, NoVoucherPool, NoVoucherAvailable,
)


class APIError(Exception):
    code = 500

    def __init__(self, message, code=None):
        super(APIError, self).__init__(message)
        if code is not None:
            self.code = code


class BadRequestParams(APIError):
    code = 400


_airtime_service_app = Klein()


class AirtimeServiceApp(object):
    app = _airtime_service_app

    def __init__(self, conn_str, reactor):
        self.engine = get_engine(conn_str, reactor)

    def handler(*args, **kw):

        def decorator(func):
            func = inlineCallbacks(func)
            route = _airtime_service_app.route(*args, **kw)

            @wraps(func)
            def wrapper(self, request, *args, **kw):
                d = func(self, request, *args, **kw)
                d.addErrback(self.handle_api_error, request)
                return d
            return route(wrapper)
        return decorator

    def handle_api_error(self, failure, request):
        error = failure.value
        if not failure.check(APIError):
            # TODO: Log something appropriately dire.
            error = APIError('Internal server error.')
        return self.format_error(request, error)

    def _get_request_id(self, request):
        try:
            return request.__request_id
        except AttributeError:
            return None

    def format_response(self, request, **params):
        request.setHeader('Content-Type', 'application/json')
        params['request_id'] = self._get_request_id(request)
        return json.dumps(params)

    def format_error(self, request, error):
        request.setHeader('Content-Type', 'application/json')
        request.setResponseCode(error.code)
        return json.dumps({
            'request_id': self._get_request_id(request),
            'error': error.message,
        })

    def _get_params(self, params, mandatory, optional):
        missing = set(mandatory) - set(params.keys())
        extra = set(params.keys()) - (set(mandatory) | set(optional))
        if missing:
            raise BadRequestParams("Missing request parameters: '%s'" % (
                "', '".join(sorted(missing))))
        if extra:
            raise BadRequestParams("Unexpected request parameters: '%s'" % (
                "', '".join(sorted(extra))))
        return params

    def get_json_params(self, request, mandatory, optional=()):
        return self._get_params(
            json.loads(request.content.read()), mandatory, optional)

    def get_url_params(self, request, mandatory, optional=()):
        if 'request_id' in request.args:
            request.__request_id = request.args['request_id'][0]
        params = self._get_params(request.args, mandatory, optional)
        return dict((k, v[0]) for k, v in params.iteritems())

    @handler(
        '/<string:voucher_pool>/issue/<string:operator>/<string:request_id>',
        methods=['PUT'])
    def issue_voucher(self, request, voucher_pool, operator, request_id):
        request.__request_id = request_id  # Name-mangled because it's not ours
        params = self.get_json_params(
            request, ['transaction_id', 'user_id', 'denomination'])
        audit_params = {
            'request_id': request_id,
            'transaction_id': params.pop('transaction_id'),
            'user_id': params.pop('user_id'),
        }
        conn = yield self.engine.connect()
        pool = VoucherPool(voucher_pool, conn)
        try:
            voucher = yield pool.issue_voucher(
                operator, params['denomination'], audit_params)
        except NoVoucherPool:
            raise APIError('Voucher pool does not exist.', 404)
        except NoVoucherAvailable:
            # This is a normal condition, so we still return a 200 OK.
            raise APIError('No voucher available.', 200)
        finally:
            yield conn.close()

        returnValue(self.format_response(request, voucher=voucher['voucher']))

    @handler('/<string:voucher_pool>/audit_query', methods=['GET'])
    def audit_query(self, request, voucher_pool):
        params = self.get_url_params(
            request, ['field', 'value'], ['request_id'])
        if params['field'] not in ['request_id', 'transaction_id', 'user_id']:
            raise BadRequestParams('Invalid audit field.')

        conn = yield self.engine.connect()
        pool = VoucherPool(voucher_pool, conn)
        try:
            query = {
                'request_id': pool.query_by_request_id,
                'transaction_id': pool.query_by_transaction_id,
                'user_id': pool.query_by_user_id,
            }[params['field']]
            rows = yield query(params['value'])
        finally:
            yield conn.close()

        results = [{
            'request_id': row['request_id'],
            'transaction_id': row['transaction_id'],
            'user_id': row['user_id'],
            'request_data': row['request_data'],
            'response_data': row['response_data'],
            'error': row['error'],
            'created_at': row['created_at'].isoformat(),
        } for row in rows]
        returnValue(self.format_response(request, results=results))

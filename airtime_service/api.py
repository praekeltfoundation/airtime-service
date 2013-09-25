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


def error_handling_request(func):
    func = inlineCallbacks(func)

    @wraps(func)
    def wrapper(self, request, *args, **kw):
        d = func(self, request, *args, **kw)
        d.addErrback(self.handle_api_error, request)
        return d
    return wrapper


class AirtimeServiceApp(object):
    AUDIT_PARAM_FIELDS = ('request_id', 'transaction_id', 'user_id')

    app = Klein()

    def __init__(self, conn_str, reactor):
        self.engine = get_engine(conn_str, reactor)

    def _parse_params(self, request, fields):
        params = {}
        for field in fields:
            [value] = request.args[field]
            params[field] = value
        return params

    def parse_audit_params(self, request):
        return self._parse_params(request, self.AUDIT_PARAM_FIELDS)

    def parse_request_params(self, request, fields, with_audit_params=True):
        req_fields = set(request.args.keys())
        expected_fields = set(fields)
        expected_fields.add('request_id')
        if with_audit_params:
            expected_fields.update(self.AUDIT_PARAM_FIELDS)

        missing_fields = expected_fields - req_fields
        if missing_fields:
            raise BadRequestParams("Missing request parameters: '%s'" % (
                "', '".join(sorted(missing_fields)),))

        extra_fields = req_fields - expected_fields
        if extra_fields:
            raise BadRequestParams("Unexpected request parameters: '%s'" % (
                "', '".join(sorted(extra_fields)),))

        params = self._parse_params(request, fields)
        if with_audit_params:
            return (self.parse_audit_params(request), params)
        return params

    def format_response(self, request, **params):
        request.setHeader('Content-Type', 'application/json')
        params['request_id'] = request.args['request_id'][0]
        return json.dumps(params)

    def format_error(self, request, error):
        request.setHeader('Content-Type', 'application/json')
        request.setResponseCode(error.code)
        return json.dumps({
            'request_id': request.args.get('request_id', [None])[0],
            'error': error.message,
        })

    def handle_api_error(self, failure, request):
        error = failure.value
        if not failure.check(APIError):
            # TODO: Log something appropriately dire.
            error = APIError('Internal server error.')
        return self.format_error(request, error)

    @app.route('/<string:voucher_pool>/issue', methods=['POST'])
    @error_handling_request
    def issue_voucher(self, request, voucher_pool):
        audit_params, params = self.parse_request_params(
            request, ['operator', 'denomination'])
        conn = yield self.engine.connect()
        pool = VoucherPool(voucher_pool, conn)
        try:
            voucher = yield pool.issue_voucher(
                params['operator'], params['denomination'], audit_params)
        except NoVoucherPool:
            raise APIError('Voucher pool does not exist.', 404)
        except NoVoucherAvailable:
            # This is a normal condition, so we still return a 200 OK.
            raise APIError('No voucher available.', 200)
        finally:
            yield conn.close()

        returnValue(self.format_response(request, voucher=voucher['voucher']))

    @app.route('/<string:voucher_pool>/audit_query', methods=['GET'])
    @error_handling_request
    def audit_query(self, request, voucher_pool):
        params = self.parse_request_params(
            request, ['field', 'value'], with_audit_params=False)
        if params['field'] not in self.AUDIT_PARAM_FIELDS:
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

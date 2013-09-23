import json

from klein import Klein

from twisted.internet.defer import inlineCallbacks, returnValue

from airtime_service.models import (
    get_engine, VoucherPool, NoVoucherPool, NoVoucherAvailable,
)


class AirtimeServiceApp(object):
    app = Klein()

    def __init__(self, conn_str, reactor):
        self.engine = get_engine(conn_str, reactor)

    def _audit_params(self, request):
        return {
            'request_id': request.args['request_id'][0],
            'transaction_id': request.args['transaction_id'][0],
            'user_id': request.args['user_id'][0],
        }

    def format_response(self, audit_params, **params):
        params['request_id'] = audit_params['request_id']
        return json.dumps(params)

    @app.route('/<string:voucher_pool>/issue', methods=['POST'])
    @inlineCallbacks
    def issue_voucher(self, request, voucher_pool):
        audit_params = self._audit_params(request)
        [operator] = request.args['operator']
        [denomination] = request.args['denomination']
        conn = yield self.engine.connect()
        pool = VoucherPool(voucher_pool, conn)
        try:
            voucher = yield pool.issue_voucher(
                operator, denomination, audit_params)
        except NoVoucherPool:
            request.setResponseCode(404)
            returnValue(self.format_response(
                audit_params, error='Voucher pool does not exist.'))
        except NoVoucherAvailable:
            # This is a normal condition, so we still return a 200 OK.
            returnValue(self.format_response(
                audit_params, error='No voucher available.'))
        finally:
            yield conn.close()

        request.setHeader('Content-Type', 'application/json')
        returnValue(self.format_response(
            audit_params, voucher=voucher['voucher']))

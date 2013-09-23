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

    @app.route('/<string:voucher_pool>/issue', methods=['POST'])
    @inlineCallbacks
    def issue_voucher(self, request, voucher_pool):
        [request_id] = request.args['request_id']
        [operator] = request.args['operator']
        [denomination] = request.args['denomination']
        conn = yield self.engine.connect()
        pool = VoucherPool(voucher_pool, conn)
        try:
            voucher = yield pool.issue_voucher(operator, denomination)
        except NoVoucherPool:
            request.setResponseCode(404)
            returnValue(json.dumps({
                'request_id': request_id,
                'error': 'Voucher pool does not exist.',
            }))
        except NoVoucherAvailable:
            # This is a normal condition, so we still return a 200 OK.
            returnValue(json.dumps({
                'request_id': request_id,
                'error': 'No voucher available.',
            }))
        finally:
            yield conn.close()

        request.setHeader('Content-Type', 'application/json')
        returnValue(json.dumps({
            'request_id': request_id,
            'voucher': voucher['voucher'],
        }))

from StringIO import StringIO
import csv
from hashlib import md5

from aludel.database import get_engine
from aludel.service import (
    service, handler, get_url_params, get_json_params, set_request_id,
    APIError, BadRequestParams,
)

from twisted.internet.defer import inlineCallbacks, returnValue

from .models import (
    VoucherPool, NoVoucherPool, NoVoucherAvailable, AuditMismatch,
)


@service
class AirtimeServiceApp(object):
    def __init__(self, conn_str, reactor):
        self.engine = get_engine(conn_str, reactor)

    def handle_api_error(self, failure, request):
        if failure.check(NoVoucherPool):
            raise APIError('Voucher pool does not exist.', 404)
        if failure.check(AuditMismatch):
            raise BadRequestParams(
                "This request has already been performed with different"
                " parameters.")
        return failure

    @handler(
        '/<string:voucher_pool>/issue/<string:operator>/<string:request_id>',
        methods=['PUT'])
    @inlineCallbacks
    def issue_voucher(self, request, voucher_pool, operator, request_id):
        set_request_id(request, request_id)
        params = get_json_params(
            request, ['transaction_id', 'user_id', 'denomination'])
        audit_params = {
            'request_id': request_id,
            'transaction_id': params.pop('transaction_id'),
            'user_id': params.pop('user_id'),
        }
        conn = yield self.engine.connect()
        try:
            pool = VoucherPool(voucher_pool, conn)
            voucher = yield pool.issue_voucher(
                operator, params['denomination'], audit_params)
        except NoVoucherAvailable:
            raise APIError('No voucher available.', 500)
        finally:
            yield conn.close()

        returnValue({'voucher': voucher['voucher']})

    @handler('/<string:voucher_pool>/audit_query', methods=['GET'])
    @inlineCallbacks
    def audit_query(self, request, voucher_pool):
        params = get_url_params(
            request, ['field', 'value'], ['request_id'])
        if params['field'] not in ['request_id', 'transaction_id', 'user_id']:
            raise BadRequestParams('Invalid audit field.')

        conn = yield self.engine.connect()
        try:
            pool = VoucherPool(voucher_pool, conn)
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
        returnValue({'results': results})

    @handler(
        '/<string:voucher_pool>/import/<string:request_id>', methods=['PUT'])
    @inlineCallbacks
    def import_vouchers(self, request, voucher_pool, request_id):
        set_request_id(request, request_id)
        content_md5 = request.requestHeaders.getRawHeaders('Content-MD5')
        if content_md5 is None:
            raise BadRequestParams("Missing Content-MD5 header.")
        content_md5 = content_md5[0].lower()
        content = request.content.read()
        if content_md5 != md5(content).hexdigest().lower():
            raise BadRequestParams(
                "Content-MD5 header does not match content.")

        reader = csv.DictReader(StringIO(content))
        row_iter = lowercase_row_keys(reader)

        conn = yield self.engine.connect()
        try:
            pool = VoucherPool(voucher_pool, conn)
            yield pool.import_vouchers(request_id, content_md5, row_iter)
        finally:
            yield conn.close()

        request.setResponseCode(201)
        returnValue({'imported': True})

    @handler('/<string:voucher_pool>/voucher_counts', methods=['GET'])
    @inlineCallbacks
    def voucher_counts(self, request, voucher_pool):
        # This sets the request_id on the request object.
        get_url_params(request, [], ['request_id'])

        conn = yield self.engine.connect()
        try:
            pool = VoucherPool(voucher_pool, conn)
            rows = yield pool.count_vouchers()
        finally:
            yield conn.close()

        results = [{
            'operator': row['operator'],
            'denomination': row['denomination'],
            'used': row['used'],
            'count': row['count'],
        } for row in rows]
        returnValue({'voucher_counts': results})

    @handler(
        '/<string:voucher_pool>/export/<string:request_id>', methods=['PUT'])
    @inlineCallbacks
    def export_vouchers(self, request, voucher_pool, request_id):
        set_request_id(request, request_id)
        params = get_json_params(
            request, [], ['count', 'operators', 'denominations'])
        conn = yield self.engine.connect()
        try:
            pool = VoucherPool(voucher_pool, conn)
            response = yield pool.export_vouchers(
                request_id, params.get('count'), params.get('operators'),
                params.get('denominations'))
        finally:
            yield conn.close()

        returnValue({
            'vouchers': response['vouchers'],
            'warnings': response['warnings'],
        })


def lowercase_row_keys(rows):
    for row in rows:
        yield dict((k.lower(), v) for k, v in row.iteritems())

from StringIO import StringIO
import csv
from hashlib import md5
import json

from twisted.internet.defer import inlineCallbacks, returnValue

from airtime_service.models import (
    get_engine, VoucherPool, NoVoucherPool, NoVoucherAvailable, AuditMismatch,
)
from airtime_service.aludel import Service, APIError, BadRequestParams


@Service.service
class AirtimeServiceApp(object):
    def __init__(self, conn_str, reactor):
        self.engine = get_engine(conn_str, reactor)

    def handle_api_error(self, failure, request):
        # failure.printTraceback()
        if failure.check(NoVoucherPool):
            raise APIError('Voucher pool does not exist.', 404)
        if failure.check(AuditMismatch):
            raise BadRequestParams(
                "This request has already been performed with different"
                " parameters.")
        return failure

    def get_json_params(self, request, mandatory, optional=()):
        return Service.get_params(
            json.loads(request.content.read()), mandatory, optional)

    def get_url_params(self, request, mandatory, optional=()):
        if 'request_id' in request.args:
            Service.set_request_id(request, request.args['request_id'][0])
        params = Service.get_params(request.args, mandatory, optional)
        return dict((k, v[0]) for k, v in params.iteritems())

    @Service.handler(
        '/<string:voucher_pool>/issue/<string:operator>/<string:request_id>',
        methods=['PUT'])
    @inlineCallbacks
    def issue_voucher(self, request, voucher_pool, operator, request_id):
        Service.set_request_id(request, request_id)
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
        except NoVoucherAvailable:
            # This is a normal condition, so we still return a 200 OK.
            raise APIError('No voucher available.', 200)
        finally:
            yield conn.close()

        returnValue({'voucher': voucher['voucher']})

    @Service.handler('/<string:voucher_pool>/audit_query', methods=['GET'])
    @inlineCallbacks
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
        returnValue({'results': results})

    @Service.handler(
        '/<string:voucher_pool>/import/<string:request_id>', methods=['PUT'])
    @inlineCallbacks
    def import_vouchers(self, request, voucher_pool, request_id):
        Service.set_request_id(request, request_id)
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
        pool = VoucherPool(voucher_pool, conn)
        try:
            yield pool.import_vouchers(request_id, content_md5, row_iter)
        finally:
            yield conn.close()

        request.setResponseCode(201)
        returnValue({'imported': True})

    @Service.handler('/<string:voucher_pool>/voucher_counts', methods=['GET'])
    @inlineCallbacks
    def voucher_counts(self, request, voucher_pool):
        # This sets the request_id on the request object.
        self.get_url_params(request, [], ['request_id'])

        conn = yield self.engine.connect()
        pool = VoucherPool(voucher_pool, conn)
        try:
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

    @Service.handler(
        '/<string:voucher_pool>/export/<string:request_id>', methods=['PUT'])
    @inlineCallbacks
    def export_vouchers(self, request, voucher_pool, request_id):
        Service.set_request_id(request, request_id)
        params = self.get_json_params(
            request, [], ['count', 'operators', 'denominations'])
        conn = yield self.engine.connect()
        pool = VoucherPool(voucher_pool, conn)
        try:
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

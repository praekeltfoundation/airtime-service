from datetime import datetime
import json
from urllib import urlencode
from StringIO import StringIO

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from twisted.web.client import Agent, FileBodyProducer, readBody
from twisted.web.http_headers import Headers
from twisted.web.server import Site

from airtime_service.api import AirtimeServiceApp
from airtime_service.models import VoucherPool

from .helpers import populate_pool, mk_audit_params


class TestVoucherPool(TestCase):
    timeout = 5

    @inlineCallbacks
    def setUp(self):
        self.asapp = AirtimeServiceApp("sqlite://", reactor=reactor)
        site = Site(self.asapp.app.resource())
        self.listener = reactor.listenTCP(0, site, interface='localhost')
        self.listener_port = self.listener.getHost().port
        self.conn = yield self.asapp.engine.connect()
        self.pool = VoucherPool('testpool', self.conn)

    def tearDown(self):
        return self.listener.loseConnection()

    def make_url(self, url_path):
        return 'http://localhost:%s/%s' % (
            self.listener_port, url_path.lstrip('/'))

    def _make_call(self, method, url_path, headers, body, expected_code):
        agent = Agent(reactor)
        url = self.make_url(url_path)
        d = agent.request(method, url, headers, body)
        return d.addCallback(self._get_response_body, expected_code)

    def _get_response_body(self, response, expected_code):
        assert response.code == expected_code
        return readBody(response).addCallback(json.loads)

    def get(self, url_path, params, expected_code=200):
        url_path = '?'.join([url_path, urlencode(params)])
        return self._make_call('GET', url_path, None, None, expected_code)

    def put(self, url_path, params, expected_code=200):
        body = FileBodyProducer(StringIO(json.dumps(params)))
        headers = Headers({
            'Content-Type': ['application/json'],
        })
        return self._make_call('PUT', url_path, headers, body, expected_code)

    def put_issue(self, request_id, operator, denomination, expected_code=200):
        params = mk_audit_params(request_id)
        params.update({
            'denomination': denomination,
        })
        params.pop('request_id')
        url_path = 'testpool/issue/%s/%s' % (operator, request_id)
        return self.put(url_path, params, expected_code)

    def get_audit_query(self, request_id, field, value, expected_code=200):
        params = {'request_id': request_id, 'field': field, 'value': value}
        return self.get('testpool/audit_query', params, expected_code)

    @inlineCallbacks
    def test_request_missing_params(self):
        params = mk_audit_params('req-0')
        params.pop('request_id')
        rsp = yield self.put(
            'testpool/issue/Tank/req-0', params, expected_code=400)
        assert rsp == {
            'request_id': 'req-0',
            'error': "Missing request parameters: 'denomination'",
        }

    @inlineCallbacks
    def test_request_missing_audit_params(self):
        params = {'denomination': 'red'}
        rsp = yield self.put(
            'testpool/issue/Tank/req-0', params, expected_code=400)
        assert rsp == {
            'request_id': 'req-0',
            'error': (
                "Missing request parameters: 'transaction_id', 'user_id'"),
        }

    @inlineCallbacks
    def test_request_extra_params(self):
        params = mk_audit_params('req-0')
        params.pop('request_id')
        params.update({
            'denomination': 'red',
            'foo': 'bar',
        })
        rsp = yield self.put(
            'testpool/issue/Tank/req-0', params, expected_code=400)
        assert rsp == {
            'request_id': 'req-0',
            'error': "Unexpected request parameters: 'foo'",
        }

    @inlineCallbacks
    def test_issue_missing_pool(self):
        rsp = yield self.put_issue('req-0', 'Tank', 'red', expected_code=404)
        assert rsp == {
            'request_id': 'req-0',
            'error': 'Voucher pool does not exist.',
        }

    @inlineCallbacks
    def test_issue_response_contains_request_id(self):
        yield populate_pool(self.pool, ['Tank'], ['red'], [0, 1])
        rsp0 = yield self.put_issue('req-0', 'Tank', 'red')
        assert rsp0['request_id'] == 'req-0'

    @inlineCallbacks
    def test_issue(self):
        yield populate_pool(self.pool, ['Tank'], ['red'], [0, 1])
        rsp0 = yield self.put_issue('req-0', 'Tank', 'red')
        assert set(rsp0.keys()) == set(['request_id', 'voucher'])
        assert rsp0['request_id'] == 'req-0'
        assert rsp0['voucher'] in ['Tank-red-0', 'Tank-red-1']

        rsp1 = yield self.put_issue('req-1', 'Tank', 'red')
        assert set(rsp1.keys()) == set(['request_id', 'voucher'])
        assert rsp1['request_id'] == 'req-1'
        assert rsp1['voucher'] in ['Tank-red-0', 'Tank-red-1']

        assert rsp0['voucher'] != rsp1['voucher']

    @inlineCallbacks
    def test_issue_no_voucher(self):
        yield populate_pool(self.pool, ['Tank'], ['red'], [0])
        rsp = yield self.put_issue('req-0', 'Tank', 'blue')
        assert rsp == {
            'request_id': 'req-0',
            'error': 'No voucher available.',
        }

    def _assert_audit_entries(self, request_id, response, expected_entries):
        def created_ats():
            for result in response['results']:
                yield datetime.strptime(
                    result['created_at'], '%Y-%m-%dT%H:%M:%S.%f').isoformat()
            while True:
                yield None

        expected_results = [{
            'request_id': entry['audit_params']['request_id'],
            'transaction_id': entry['audit_params']['transaction_id'],
            'user_id': entry['audit_params']['user_id'],
            'request_data': entry['request_data'],
            'response_data': entry['response_data'],
            'error': entry['error'],
            'created_at': created_at,
        } for entry, created_at in zip(expected_entries, created_ats())]

        assert response == {
            'request_id': request_id,
            'results': expected_results,
        }

    @inlineCallbacks
    def test_query_by_request_id(self):
        yield self.pool.create_tables()

        audit_params = mk_audit_params('req-0')
        rsp = yield self.get_audit_query('audit-0', 'request_id', 'req-0')
        assert rsp == {
            'request_id': 'audit-0',
            'results': [],
        }

        yield self.pool._audit_request(audit_params, 'req_data', 'resp_data')

        rsp = yield self.get_audit_query('audit-1', 'request_id', 'req-0')

        self._assert_audit_entries('audit-1', rsp, [{
            'audit_params': audit_params,
            'request_data': u'req_data',
            'response_data': u'resp_data',
            'error': False,
        }])

    @inlineCallbacks
    def test_query_by_transaction_id(self):
        yield self.pool.create_tables()

        audit_params_0 = mk_audit_params('req-0', 'transaction-0')
        audit_params_1 = mk_audit_params('req-1', 'transaction-0')
        rsp = yield self.get_audit_query(
            'audit-0', 'transaction_id', 'transaction-0')
        assert rsp == {
            'request_id': 'audit-0',
            'results': [],
        }

        yield self.pool._audit_request(
            audit_params_0, 'req_data_0', 'resp_data_0')
        yield self.pool._audit_request(
            audit_params_1, 'req_data_1', 'resp_data_1')

        rsp = yield self.get_audit_query(
            'audit-1', 'transaction_id', 'transaction-0')

        self._assert_audit_entries('audit-1', rsp, [{
            'audit_params': audit_params_0,
            'request_data': u'req_data_0',
            'response_data': u'resp_data_0',
            'error': False,
        }, {
            'audit_params': audit_params_1,
            'request_data': u'req_data_1',
            'response_data': u'resp_data_1',
            'error': False,
        }])

    @inlineCallbacks
    def test_query_by_user_id(self):
        yield self.pool.create_tables()

        audit_params_0 = mk_audit_params('req-0', 'transaction-0', 'user-0')
        audit_params_1 = mk_audit_params('req-1', 'transaction-1', 'user-0')
        rsp = yield self.get_audit_query('audit-0', 'user_id', 'user-0')
        assert rsp == {
            'request_id': 'audit-0',
            'results': [],
        }

        yield self.pool._audit_request(
            audit_params_0, 'req_data_0', 'resp_data_0')
        yield self.pool._audit_request(
            audit_params_1, 'req_data_1', 'resp_data_1')

        rsp = yield self.get_audit_query('audit-1', 'user_id', 'user-0')

        self._assert_audit_entries('audit-1', rsp, [{
            'audit_params': audit_params_0,
            'request_data': u'req_data_0',
            'response_data': u'resp_data_0',
            'error': False,
        }, {
            'audit_params': audit_params_1,
            'request_data': u'req_data_1',
            'response_data': u'resp_data_1',
            'error': False,
        }])

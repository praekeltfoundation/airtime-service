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

    def post(self, url_path, params, expected_code=200):
        agent = Agent(reactor)
        url = self.make_url(url_path)
        headers = Headers({
            'Content-Type': ['application/x-www-form-urlencoded'],
        })
        body = FileBodyProducer(StringIO(urlencode(params)))
        d = agent.request('POST', url, headers, body)
        return d.addCallback(self._get_response_body, expected_code)

    def _get_response_body(self, response, expected_code):
        assert response.code == expected_code
        return readBody(response).addCallback(json.loads)

    def post_issue(self, request_id, operator, denomination,
                   expected_code=200):
        params = mk_audit_params(request_id)
        params.update({
            'operator': operator,
            'denomination': denomination,
        })
        return self.post('testpool/issue', params, expected_code)

    @inlineCallbacks
    def test_request_missing_params(self):
        params = mk_audit_params('req-0')
        rsp = yield self.post('testpool/issue', params, expected_code=400)
        assert rsp == {
            'request_id': 'req-0',
            'error': "Missing request parameters: 'denomination', 'operator'",
        }

    @inlineCallbacks
    def test_request_missing_audit_params(self):
        params = {'operator': 'Tank', 'denomination': 'red'}
        rsp = yield self.post('testpool/issue', params, expected_code=400)
        assert rsp == {
            'request_id': None,
            'error': (
                "Missing request parameters:"
                " 'request_id', 'transaction_id', 'user_id'"),
        }

    @inlineCallbacks
    def test_request_extra_params(self):
        params = mk_audit_params('req-0')
        params.update({
            'operator': 'Tank',
            'denomination': 'red',
            'foo': 'bar',
        })
        rsp = yield self.post('testpool/issue', params, expected_code=400)
        assert rsp == {
            'request_id': 'req-0',
            'error': "Unexpected request parameters: 'foo'",
        }

    @inlineCallbacks
    def test_issue_missing_pool(self):
        rsp = yield self.post_issue('req-0', 'Tank', 'red', expected_code=404)
        assert rsp == {
            'request_id': 'req-0',
            'error': 'Voucher pool does not exist.',
        }

    @inlineCallbacks
    def test_issue_response_contains_request_id(self):
        yield populate_pool(self.pool, ['Tank'], ['red'], [0, 1])
        rsp0 = yield self.post_issue('req-0', 'Tank', 'red')
        assert rsp0['request_id'] == 'req-0'

    @inlineCallbacks
    def test_issue(self):
        yield populate_pool(self.pool, ['Tank'], ['red'], [0, 1])
        rsp0 = yield self.post_issue('req-0', 'Tank', 'red')
        assert set(rsp0.keys()) == set(['request_id', 'voucher'])
        assert rsp0['request_id'] == 'req-0'
        assert rsp0['voucher'] in ['Tank-red-0', 'Tank-red-1']

        rsp1 = yield self.post_issue('req-1', 'Tank', 'red')
        assert set(rsp1.keys()) == set(['request_id', 'voucher'])
        assert rsp1['request_id'] == 'req-1'
        assert rsp1['voucher'] in ['Tank-red-0', 'Tank-red-1']

        assert rsp0['voucher'] != rsp1['voucher']

    @inlineCallbacks
    def test_issue_no_voucher(self):
        yield populate_pool(self.pool, ['Tank'], ['red'], [0])
        rsp = yield self.post_issue('req-0', 'Tank', 'blue')
        assert rsp == {
            'request_id': 'req-0',
            'error': 'No voucher available.',
        }

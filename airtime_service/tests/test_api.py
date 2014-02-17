from datetime import datetime
from hashlib import md5
from itertools import izip_longest
import json
import os
from urllib import urlencode
from StringIO import StringIO

from aludel.database import MetaData
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from twisted.web.client import Agent, FileBodyProducer, readBody
from twisted.web.http_headers import Headers
from twisted.web.server import Site

from airtime_service.api import AirtimeServiceApp
from airtime_service.models import VoucherPool

from .helpers import populate_pool, mk_audit_params, sorted_dicts, voucher_dict


class ApiClient(object):
    def __init__(self, base_url):
        self._base_url = base_url

    def _make_url(self, url_path):
        return '%s/%s' % (self._base_url, url_path.lstrip('/'))

    def _make_call(self, method, url_path, headers, body, expected_code):
        agent = Agent(reactor)
        url = self._make_url(url_path)
        d = agent.request(method, url, headers, body)
        return d.addCallback(self._get_response_body, expected_code)

    def _get_response_body(self, response, expected_code):
        assert response.code == expected_code
        return readBody(response).addCallback(json.loads)

    def get(self, url_path, params, expected_code):
        url_path = '?'.join([url_path, urlencode(params)])
        return self._make_call('GET', url_path, None, None, expected_code)

    def put(self, url_path, headers, content, expected_code=200):
        body = FileBodyProducer(StringIO(content))
        return self._make_call('PUT', url_path, headers, body, expected_code)

    def put_json(self, url_path, params, expected_code=200):
        headers = Headers({'Content-Type': ['application/json']})
        return self.put(
            url_path, headers, json.dumps(params), expected_code)

    def put_issue(self, request_id, operator, denomination, expected_code=200):
        params = mk_audit_params(request_id)
        params.update({
            'denomination': denomination,
        })
        params.pop('request_id')
        url_path = 'testpool/issue/%s/%s' % (operator, request_id)
        return self.put_json(url_path, params, expected_code)

    def put_create(self, expected_code=201):
        url_path = 'testpool'
        return self.put(url_path, Headers({}), None, expected_code)

    def put_import(self, request_id, content, content_md5=None,
                   expected_code=201):
        url_path = 'testpool/import/%s' % (request_id,)
        hdict = {
            'Content-Type': ['text/csv'],
        }
        if content_md5 is None:
            content_md5 = md5(content).hexdigest()
        if content_md5:
            hdict['Content-MD5'] = [content_md5]
        return self.put(url_path, Headers(hdict), content, expected_code)

    def put_export(self, request_id, count=None, operators=None,
                   denominations=None, expected_code=200):
        params = {}
        if count is not None:
            params['count'] = count
        if operators is not None:
            params['operators'] = operators
        if denominations is not None:
            params['denominations'] = denominations
        url_path = 'testpool/export/%s' % (request_id,)
        return self.put_json(url_path, params, expected_code)

    def get_audit_query(self, request_id, field, value, expected_code=200):
        params = {'request_id': request_id, 'field': field, 'value': value}
        return self.get('testpool/audit_query', params, expected_code)

    def get_voucher_counts(self, request_id, expected_code=200):
        params = {'request_id': request_id}
        return self.get('testpool/voucher_counts', params, expected_code)


class TestAirtimeServiceApp(TestCase):
    timeout = 5

    @inlineCallbacks
    def setUp(self):
        # We need to make sure all our queries run in the same thread,
        # otherwise sqlite gets very sad.
        reactor.suggestThreadPoolSize(1)
        connection_string = os.environ.get(
            "ALUDEL_TEST_CONNECTION_STRING", "sqlite://")
        self._using_mysql = connection_string.startswith('mysql')
        self.asapp = AirtimeServiceApp(connection_string, reactor=reactor)
        site = Site(self.asapp.app.resource())
        self.listener = reactor.listenTCP(0, site, interface='localhost')
        self.listener_port = self.listener.getHost().port
        self._drop_tables()
        self.conn = yield self.asapp.engine.connect()
        self.pool = VoucherPool('testpool', self.conn)
        self.client = ApiClient('http://localhost:%s' % (self.listener_port,))

    @inlineCallbacks
    def tearDown(self):
        yield self.conn.close()
        self._drop_tables()
        yield self.listener.loseConnection()

    def _drop_tables(self):
        # NOTE: This is a blocking operation!
        md = MetaData(bind=self.asapp.engine._engine)
        md.reflect()
        md.drop_all()
        assert self.asapp.engine._engine.table_names() == []

    @inlineCallbacks
    def assert_voucher_counts(self, expected_rows):
        rows = yield self.pool.count_vouchers()
        assert sorted(tuple(r) for r in rows) == sorted(expected_rows)

    @inlineCallbacks
    def test_request_missing_params(self):
        params = mk_audit_params('req-0')
        params.pop('request_id')
        rsp = yield self.client.put_json(
            'testpool/issue/Tank/req-0', params, expected_code=400)
        assert rsp == {
            'request_id': 'req-0',
            'error': "Missing request parameters: 'denomination'",
        }

    @inlineCallbacks
    def test_request_missing_audit_params(self):
        params = {'denomination': 'red'}
        rsp = yield self.client.put_json(
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
        rsp = yield self.client.put_json(
            'testpool/issue/Tank/req-0', params, expected_code=400)
        assert rsp == {
            'request_id': 'req-0',
            'error': "Unexpected request parameters: 'foo'",
        }

    @inlineCallbacks
    def test_issue_missing_pool(self):
        rsp = yield self.client.put_issue(
            'req-0', 'Tank', 'red', expected_code=404)
        assert rsp == {
            'request_id': 'req-0',
            'error': 'Voucher pool does not exist.',
        }

    @inlineCallbacks
    def test_issue_response_contains_request_id(self):
        yield self.pool.create_tables()
        yield populate_pool(self.pool, ['Tank'], ['red'], [0, 1])
        rsp0 = yield self.client.put_issue('req-0', 'Tank', 'red')
        assert rsp0['request_id'] == 'req-0'

    @inlineCallbacks
    def test_issue(self):
        yield self.pool.create_tables()
        yield populate_pool(self.pool, ['Tank'], ['red'], [0, 1])
        rsp0 = yield self.client.put_issue('req-0', 'Tank', 'red')
        assert set(rsp0.keys()) == set(['request_id', 'voucher'])
        assert rsp0['request_id'] == 'req-0'
        assert rsp0['voucher'] in ['Tank-red-0', 'Tank-red-1']

        rsp1 = yield self.client.put_issue('req-1', 'Tank', 'red')
        assert set(rsp1.keys()) == set(['request_id', 'voucher'])
        assert rsp1['request_id'] == 'req-1'
        assert rsp1['voucher'] in ['Tank-red-0', 'Tank-red-1']

        assert rsp0['voucher'] != rsp1['voucher']

    @inlineCallbacks
    def test_issue_idempotent(self):
        yield self.pool.create_tables()
        yield populate_pool(self.pool, ['Tank'], ['red'], [0])
        rsp0 = yield self.client.put_issue('req-0', 'Tank', 'red')
        assert rsp0 == {
            'request_id': 'req-0',
            'voucher': 'Tank-red-0',
        }

        yield populate_pool(self.pool, ['Tank'], ['red'], [1])

        rsp1 = yield self.client.put_issue('req-0', 'Tank', 'red')
        assert rsp1 == {
            'request_id': 'req-0',
            'voucher': 'Tank-red-0',
        }

        rsp2 = yield self.client.put_issue('req-1', 'Tank', 'red')
        assert rsp2 == {
            'request_id': 'req-1',
            'voucher': 'Tank-red-1',
        }

        rsp3 = yield self.client.put_issue(
            'req-1', 'Tank', 'blue', expected_code=400)
        assert rsp3 == {
            'request_id': 'req-1',
            'error': (
                'This request has already been performed with different'
                ' parameters.'),
        }

    @inlineCallbacks
    def test_issue_no_voucher(self):
        yield self.pool.create_tables()
        yield populate_pool(self.pool, ['Tank'], ['red'], [0])
        rsp = yield self.client.put_issue(
            'req-0', 'Tank', 'blue', expected_code=500)
        assert rsp == {
            'request_id': 'req-0',
            'error': 'No voucher available.',
        }

    def _assert_audit_entries(self, request_id, response, expected_entries):
        def created_ats():
            for result in response['results']:
                yield datetime.strptime(
                    result['created_at'], '%Y-%m-%dT%H:%M:%S.%f').isoformat()

        expected_results = [{
            'request_id': entry['audit_params']['request_id'],
            'transaction_id': entry['audit_params']['transaction_id'],
            'user_id': entry['audit_params']['user_id'],
            'request_data': entry['request_data'],
            'response_data': entry['response_data'],
            'error': entry['error'],
            'created_at': created_at,
        } for entry, created_at in izip_longest(
            expected_entries, created_ats())]

        assert response == {
            'request_id': request_id,
            'results': expected_results,
        }

    @inlineCallbacks
    def test_query_bad_field(self):
        yield self.pool.create_tables()

        rsp = yield self.client.get_audit_query(
            'audit-0', 'foo', 'req-0', expected_code=400)
        assert rsp == {
            'request_id': 'audit-0',
            'error': 'Invalid audit field.',
        }

    @inlineCallbacks
    def test_query_by_request_id(self):
        yield self.pool.create_tables()

        audit_params = mk_audit_params('req-0')
        rsp = yield self.client.get_audit_query(
            'audit-0', 'request_id', 'req-0')
        assert rsp == {
            'request_id': 'audit-0',
            'results': [],
        }

        yield self.pool._audit_request(audit_params, 'req_data', 'resp_data')

        rsp = yield self.client.get_audit_query(
            'audit-1', 'request_id', 'req-0')

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
        rsp = yield self.client.get_audit_query(
            'audit-0', 'transaction_id', 'transaction-0')
        assert rsp == {
            'request_id': 'audit-0',
            'results': [],
        }

        yield self.pool._audit_request(
            audit_params_0, 'req_data_0', 'resp_data_0')
        yield self.pool._audit_request(
            audit_params_1, 'req_data_1', 'resp_data_1')

        rsp = yield self.client.get_audit_query(
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
        rsp = yield self.client.get_audit_query('audit-0', 'user_id', 'user-0')
        assert rsp == {
            'request_id': 'audit-0',
            'results': [],
        }

        yield self.pool._audit_request(
            audit_params_0, 'req_data_0', 'resp_data_0')
        yield self.pool._audit_request(
            audit_params_1, 'req_data_1', 'resp_data_1')

        rsp = yield self.client.get_audit_query('audit-1', 'user_id', 'user-0')

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
    def test_create(self):
        resp = yield self.client.put_create()
        assert resp == {
            'request_id': None,
            'created': True,
        }
        # Recreating a pool has a different response.
        resp = yield self.client.put_create(expected_code=200)
        assert resp == {
            'request_id': None,
            'created': False,
        }

    @inlineCallbacks
    def test_import(self):
        yield self.pool.create_tables()
        yield self.assert_voucher_counts([])

        content = '\n'.join([
            'operator,denomination,voucher',
            'Tank,red,Tr0',
            'Tank,red,Tr1',
            'Tank,blue,Tb0',
            'Tank,blue,Tb1',
            'Link,red,Lr0',
            'Link,red,Lr1',
            'Link,blue,Lb0',
            'Link,blue,Lb1',
        ])

        resp = yield self.client.put_import('req-0', content)
        assert resp == {
            'request_id': 'req-0',
            'imported': True,
        }
        yield self.assert_voucher_counts([
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 2),
            ('Tank', 'red', False, 2),
        ])

    @inlineCallbacks
    def test_import_missing_pool(self):
        content = '\n'.join([
            'operator,denomination,voucher',
            'Tank,red,Tr0',
            'Tank,red,Tr1',
            'Tank,blue,Tb0',
            'Tank,blue,Tb1',
            'Link,red,Lr0',
            'Link,red,Lr1',
            'Link,blue,Lb0',
            'Link,blue,Lb1',
        ])

        rsp = yield self.client.put_import('req-0', content, expected_code=404)
        assert rsp == {
            'request_id': 'req-0',
            'error': 'Voucher pool does not exist.',
        }

    @inlineCallbacks
    def test_import_heading_case_mismatch(self):
        yield self.pool.create_tables()
        yield self.assert_voucher_counts([])

        content = '\n'.join([
            'OperAtor,denomInation,voucheR',
            'Tank,red,Tr0',
            'Tank,red,Tr1',
            'Tank,blue,Tb0',
            'Tank,blue,Tb1',
            'Link,red,Lr0',
            'Link,red,Lr1',
            'Link,blue,Lb0',
            'Link,blue,Lb1',
        ])

        resp = yield self.client.put_import('req-0', content)
        assert resp == {
            'request_id': 'req-0',
            'imported': True,
        }
        yield self.assert_voucher_counts([
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 2),
            ('Tank', 'red', False, 2),
        ])

    @inlineCallbacks
    def test_import_no_content_md5(self):
        yield self.pool.create_tables()

        resp = yield self.client.put_import(
            'req-0', 'content', '', expected_code=400)
        assert resp == {
            'request_id': 'req-0',
            'error': 'Missing Content-MD5 header.',
        }

    @inlineCallbacks
    def test_import_bad_content_md5(self):
        yield self.pool.create_tables()

        resp = yield self.client.put_import(
            'req-0', 'content', 'badmd5', expected_code=400)
        assert resp == {
            'request_id': 'req-0',
            'error': 'Content-MD5 header does not match content.',
        }

    @inlineCallbacks
    def test_import_idempotent(self):
        yield self.pool.create_tables()
        yield self.assert_voucher_counts([])

        content = '\n'.join([
            'operator,denomination,voucher',
            'Tank,red,Tr0',
            'Tank,red,Tr1',
            'Tank,blue,Tb0',
            'Tank,blue,Tb1',
            'Link,red,Lr0',
            'Link,red,Lr1',
            'Link,blue,Lb0',
            'Link,blue,Lb1',
        ])

        expected_counts = [
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 2),
            ('Tank', 'red', False, 2),
        ]

        resp = yield self.client.put_import('req-0', content)
        assert resp == {
            'request_id': 'req-0',
            'imported': True,
        }
        yield self.assert_voucher_counts(expected_counts)

        resp = yield self.client.put_import('req-0', content)
        assert resp == {
            'request_id': 'req-0',
            'imported': True,
        }
        yield self.assert_voucher_counts(expected_counts)

        content_2 = '\n'.join([
            'operator,denomination,voucher',
            'Tank,red,Tr0',
            'Tank,red,Tr1',
            'Tank,blue,Tb0',
            'Tank,blue,Tb1',
        ])

        resp = yield self.client.put_import(
            'req-0', content_2, expected_code=400)
        assert resp == {
            'request_id': 'req-0',
            'error': (
                'This request has already been performed with different'
                ' parameters.'),
        }
        yield self.assert_voucher_counts(expected_counts)

    def _sorted_voucher_counts(self, voucher_counts):
        return sorted(voucher_counts, key=lambda vc: (
            vc['operator'], vc['denomination'], vc['used']))

    @inlineCallbacks
    def test_voucher_counts(self):
        yield self.pool.create_tables()
        rsp0 = yield self.client.get_voucher_counts('req-0')
        assert rsp0 == {
            'request_id': 'req-0',
            'voucher_counts': [],
        }

        yield populate_pool(self.pool, ['Tank'], ['red'], [0, 1])
        rsp1 = yield self.client.get_voucher_counts('req-1')
        assert rsp1 == {
            'request_id': 'req-1',
            'voucher_counts': [
                {
                    'operator': 'Tank',
                    'denomination': 'red',
                    'used': False,
                    'count': 2,
                },
            ],
        }

        yield populate_pool(self.pool, ['Link'], ['blue'], [0, 1])
        yield self.pool.issue_voucher('Link', 'blue', mk_audit_params('req-0'))
        rsp2 = yield self.client.get_voucher_counts('req-2')
        assert self._sorted_voucher_counts(rsp2['voucher_counts']) == [
            {
                'operator': 'Link',
                'denomination': 'blue',
                'used': False,
                'count': 1,
            },
            {
                'operator': 'Link',
                'denomination': 'blue',
                'used': True,
                'count': 1,
            },
            {
                'operator': 'Tank',
                'denomination': 'red',
                'used': False,
                'count': 2,
            },
        ]

    @inlineCallbacks
    def test_export_all_vouchers(self):
        yield self.pool.create_tables()
        yield populate_pool(
            self.pool, ['Tank', 'Link'], ['red', 'blue'], [0, 1])

        response = yield self.client.put_export('req-0')
        assert set(response.keys()) == set([
            'request_id', 'vouchers', 'warnings'])
        assert response['request_id'] == 'req-0'
        assert response['warnings'] == []
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'red', 'Tank-red-0'),
            voucher_dict('Tank', 'red', 'Tank-red-1'),
            voucher_dict('Tank', 'blue', 'Tank-blue-0'),
            voucher_dict('Tank', 'blue', 'Tank-blue-1'),
            voucher_dict('Link', 'red', 'Link-red-0'),
            voucher_dict('Link', 'red', 'Link-red-1'),
            voucher_dict('Link', 'blue', 'Link-blue-0'),
            voucher_dict('Link', 'blue', 'Link-blue-1'),
        ])

        yield self.assert_voucher_counts([
            ('Link', 'blue', True, 2),
            ('Link', 'red', True, 2),
            ('Tank', 'blue', True, 2),
            ('Tank', 'red', True, 2),
        ])

    @inlineCallbacks
    def test_export_some_vouchers(self):
        yield self.pool.create_tables()
        # We give all vouchers of the same type the same voucher code to avoid
        # having to check all the permutations.
        yield populate_pool(
            self.pool, ['Tank', 'Link'], ['red', 'blue'], [0, 0])
        yield self.assert_voucher_counts([
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 2),
            ('Tank', 'red', False, 2),
        ])

        response = yield self.client.put_export(
            'req-0', 1, ['Tank'], ['red', 'blue'])
        assert set(response.keys()) == set([
            'request_id', 'vouchers', 'warnings'])
        assert response['request_id'] == 'req-0'
        assert response['warnings'] == []
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'red', 'Tank-red-0'),
            voucher_dict('Tank', 'blue', 'Tank-blue-0'),
        ])

        yield self.assert_voucher_counts([
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 1),
            ('Tank', 'red', False, 1),
            ('Tank', 'blue', True, 1),
            ('Tank', 'red', True, 1),
        ])

    @inlineCallbacks
    def test_export_too_many_vouchers(self):
        yield self.pool.create_tables()
        # We give all vouchers of the same type the same voucher code to avoid
        # having to check all the permutations.
        yield populate_pool(
            self.pool, ['Tank', 'Link'], ['red', 'blue'], [0, 0])
        yield self.assert_voucher_counts([
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 2),
            ('Tank', 'red', False, 2),
        ])

        response = yield self.client.put_export(
            'req-0', 4, ['Tank'], ['red', 'blue'])
        assert set(response.keys()) == set([
            'request_id', 'vouchers', 'warnings'])
        assert response['request_id'] == 'req-0'
        assert sorted(response['warnings']) == sorted([
            "Insufficient vouchers available for 'Tank' 'red'.",
            "Insufficient vouchers available for 'Tank' 'blue'.",
        ])
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'red', 'Tank-red-0'),
            voucher_dict('Tank', 'red', 'Tank-red-0'),
            voucher_dict('Tank', 'blue', 'Tank-blue-0'),
            voucher_dict('Tank', 'blue', 'Tank-blue-0'),
        ])
        yield self.assert_voucher_counts([
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', True, 2),
            ('Tank', 'red', True, 2),
        ])

    @inlineCallbacks
    def test_export_idempotent(self):
        yield self.pool.create_tables()
        yield populate_pool(self.pool, ['Tank', 'Link'], ['red', 'blue'], [0])
        yield self.assert_voucher_counts([
            ('Link', 'blue', False, 1),
            ('Link', 'red', False, 1),
            ('Tank', 'blue', False, 1),
            ('Tank', 'red', False, 1),
        ])

        response = yield self.client.put_export('req-0', 1, ['Tank'], ['red'])
        assert set(response.keys()) == set([
            'request_id', 'vouchers', 'warnings'])
        assert response['request_id'] == 'req-0'
        assert response['warnings'] == []
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'red', 'Tank-red-0'),
        ])
        yield self.assert_voucher_counts([
            ('Link', 'blue', False, 1),
            ('Link', 'red', False, 1),
            ('Tank', 'blue', False, 1),
            ('Tank', 'red', True, 1),
        ])

        response = yield self.client.put_export('req-0', 1, ['Tank'], ['red'])
        assert set(response.keys()) == set([
            'request_id', 'vouchers', 'warnings'])
        assert response['request_id'] == 'req-0'
        assert response['warnings'] == []
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'red', 'Tank-red-0'),
        ])
        yield self.assert_voucher_counts([
            ('Link', 'blue', False, 1),
            ('Link', 'red', False, 1),
            ('Tank', 'blue', False, 1),
            ('Tank', 'red', True, 1),
        ])

        response = yield self.client.put_export(
            'req-0', 2, ['Tank'], ['red'], expected_code=400)
        assert response == {
            'request_id': 'req-0',
            'error': (
                'This request has already been performed with different'
                ' parameters.'),
        }

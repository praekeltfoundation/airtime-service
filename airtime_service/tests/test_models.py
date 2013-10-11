from datetime import datetime

from aludel.database import get_engine
from aludel.tests.doubles import FakeReactorThreads

from twisted.trial.unittest import TestCase

from airtime_service.models import (
    VoucherPool, NoVoucherAvailable, NoVoucherPool, AuditMismatch,
)

from .helpers import populate_pool, mk_audit_params, sorted_dicts, voucher_dict


class TestVoucherPool(TestCase):
    timeout = 5

    def setUp(self):
        self.engine = get_engine("sqlite://", reactor=FakeReactorThreads())
        self.conn = self.successResultOf(self.engine.connect())
        self._request_count = 0

    def tearDown(self):
        self.successResultOf(self.conn.close())

    def assert_voucher_counts(self, pool, expected_rows):
        rows = self.successResultOf(pool.count_vouchers())
        assert sorted(tuple(r) for r in rows) == sorted(expected_rows)

    def test_import_creates_table(self):
        pool = VoucherPool('testpool', self.conn)
        f = self.failureResultOf(pool.count_vouchers(), NoVoucherPool)
        assert f.value.args == ('testpool',)
        populate_pool(pool, ['Tank'], ['red'], [0])
        self.assert_voucher_counts(pool, [('Tank', 'red', False, 1)])

    def test_exists(self):
        pool = VoucherPool('testpool', self.conn)
        assert not self.successResultOf(pool.exists())
        self.successResultOf(pool.create_tables())
        assert self.successResultOf(pool.exists())

    def test_import_vouchers(self):
        pool = VoucherPool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        self.successResultOf(pool.import_vouchers('req-0', 'md5-0', [
            {'operator': 'Tank', 'denomination': 'red', 'voucher': 'Tr0'},
            {'operator': 'Tank', 'denomination': 'red', 'voucher': 'Tr1'},
            {'operator': 'Tank', 'denomination': 'blue', 'voucher': 'Tb0'},
            {'operator': 'Tank', 'denomination': 'blue', 'voucher': 'Tb1'},
            {'operator': 'Link', 'denomination': 'red', 'voucher': 'Lr0'},
            {'operator': 'Link', 'denomination': 'red', 'voucher': 'Lr1'},
            {'operator': 'Link', 'denomination': 'blue', 'voucher': 'Lb0'},
            {'operator': 'Link', 'denomination': 'blue', 'voucher': 'Lb1'},
        ]))
        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 2),
            ('Tank', 'red', False, 2),
        ])

    def test_import_vouchers_idempotence(self):
        pool = VoucherPool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        vouchers = [
            {'operator': 'Tank', 'denomination': 'red', 'voucher': 'Tr0'},
            {'operator': 'Tank', 'denomination': 'red', 'voucher': 'Tr1'},
            {'operator': 'Tank', 'denomination': 'blue', 'voucher': 'Tb0'},
            {'operator': 'Tank', 'denomination': 'blue', 'voucher': 'Tb1'},
            {'operator': 'Link', 'denomination': 'red', 'voucher': 'Lr0'},
            {'operator': 'Link', 'denomination': 'red', 'voucher': 'Lr1'},
            {'operator': 'Link', 'denomination': 'blue', 'voucher': 'Lb0'},
            {'operator': 'Link', 'denomination': 'blue', 'voucher': 'Lb1'},
        ]

        expected_vouchers = [
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 2),
            ('Tank', 'red', False, 2),
        ]

        self.successResultOf(pool.import_vouchers('req-0', 'md5-0', vouchers))
        self.assert_voucher_counts(pool, expected_vouchers)

        # Again, with the same request.
        self.successResultOf(pool.import_vouchers('req-0', 'md5-0', vouchers))
        self.assert_voucher_counts(pool, expected_vouchers)

        # Same request, different content.
        self.failureResultOf(
            pool.import_vouchers('req-0', 'md5-1', vouchers[:2]),
            AuditMismatch)
        self.assert_voucher_counts(pool, expected_vouchers)

    def test_issue_voucher(self):
        pool = VoucherPool('testpool', self.conn)
        populate_pool(pool, ['Tank'], ['red'], [0])
        self.assert_voucher_counts(pool, [('Tank', 'red', False, 1)])

        voucher = self.successResultOf(
            pool.issue_voucher('Tank', 'red', mk_audit_params('req-0')))
        assert voucher['operator'] == 'Tank'
        assert voucher['denomination'] == 'red'
        assert voucher['voucher'] == 'Tank-red-0'
        self.assert_voucher_counts(pool, [('Tank', 'red', True, 1)])

    def test_issue_voucher_not_available(self):
        pool = VoucherPool('testpool', self.conn)
        self.successResultOf(pool.create_tables())
        self.assert_voucher_counts(pool, [])
        self.failureResultOf(
            pool.issue_voucher('Tank', 'red', mk_audit_params('req-0')),
            NoVoucherAvailable)

    def test_issue_voucher_idempotent(self):
        pool = VoucherPool('testpool', self.conn)
        populate_pool(pool, ['Tank'], ['red'], [0])
        self.assert_voucher_counts(pool, [('Tank', 'red', False, 1)])

        audit_params = mk_audit_params('req-0')

        # Issue a successful request.
        voucher = self.successResultOf(
            pool.issue_voucher('Tank', 'red', audit_params))
        assert voucher['operator'] == 'Tank'
        assert voucher['denomination'] == 'red'
        assert voucher['voucher'] == 'Tank-red-0'
        self.assert_voucher_counts(pool, [('Tank', 'red', True, 1)])

        # Reissue the same request with no available vouchers.
        voucher = self.successResultOf(
            pool.issue_voucher('Tank', 'red', audit_params))
        assert voucher['operator'] == 'Tank'
        assert voucher['denomination'] == 'red'
        assert voucher['voucher'] == 'Tank-red-0'
        self.assert_voucher_counts(pool, [('Tank', 'red', True, 1)])

        # Reuse the same request_id but with different parameters.
        audit_params_2 = mk_audit_params('req-0', transaction_id='foo')
        self.failureResultOf(
            pool.issue_voucher('Tank', 'red', audit_params_2), AuditMismatch)

    def test_issue_voucher_idempotent_not_available(self):
        pool = VoucherPool('testpool', self.conn)
        self.successResultOf(pool.create_tables())
        self.assert_voucher_counts(pool, [])

        audit_params = mk_audit_params('req-0')

        # Issue an unsuccessful request.
        self.failureResultOf(
            pool.issue_voucher('Tank', 'red', audit_params),
            NoVoucherAvailable)

        populate_pool(pool, ['Tank'], ['red'], [0])
        self.assert_voucher_counts(pool, [('Tank', 'red', False, 1)])

        # Reissue the same request with available vouchers.
        self.failureResultOf(
            pool.issue_voucher('Tank', 'red', audit_params),
            NoVoucherAvailable)
        self.assert_voucher_counts(pool, [('Tank', 'red', False, 1)])

    def test_query_by_request_id(self):
        pool = VoucherPool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        audit_params = mk_audit_params('req-0')
        rows = self.successResultOf(pool.query_by_request_id('req-0'))
        assert rows == []

        before = datetime.utcnow()
        self.successResultOf(
            pool._audit_request(audit_params, 'req_data', 'resp_data'))
        self.successResultOf(
            pool._audit_request(mk_audit_params('req-excl'), 'excl', 'excl'))
        after = datetime.utcnow()

        rows = self.successResultOf(pool.query_by_request_id('req-0'))

        created_at = rows[0]['created_at']
        assert before <= created_at <= after
        assert rows == [{
            'request_id': audit_params['request_id'],
            'transaction_id': audit_params['transaction_id'],
            'user_id': audit_params['user_id'],
            'request_data': u'req_data',
            'response_data': u'resp_data',
            'error': False,
            'created_at': created_at,
        }]

    def test_query_by_transaction_id(self):
        pool = VoucherPool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        audit_params_0 = mk_audit_params('req-0', 'transaction-0')
        audit_params_1 = mk_audit_params('req-1', 'transaction-0')
        rows = self.successResultOf(
            pool.query_by_transaction_id('transaction-0'))
        assert rows == []

        before = datetime.utcnow()
        self.successResultOf(
            pool._audit_request(audit_params_0, 'req_data_0', 'resp_data_0'))
        self.successResultOf(
            pool._audit_request(audit_params_1, 'req_data_1', 'resp_data_1'))
        self.successResultOf(
            pool._audit_request(mk_audit_params('req-excl'), 'excl', 'excl'))
        after = datetime.utcnow()

        rows = self.successResultOf(
            pool.query_by_transaction_id('transaction-0'))
        created_at_0 = rows[0]['created_at']
        created_at_1 = rows[1]['created_at']
        assert before <= created_at_0 <= created_at_1 <= after

        assert rows == [{
            'request_id': audit_params_0['request_id'],
            'transaction_id': audit_params_0['transaction_id'],
            'user_id': audit_params_0['user_id'],
            'request_data': u'req_data_0',
            'response_data': u'resp_data_0',
            'error': False,
            'created_at': created_at_0,
        }, {
            'request_id': audit_params_1['request_id'],
            'transaction_id': audit_params_1['transaction_id'],
            'user_id': audit_params_1['user_id'],
            'request_data': u'req_data_1',
            'response_data': u'resp_data_1',
            'error': False,
            'created_at': created_at_1,
        }]

    def test_query_by_user_id(self):
        pool = VoucherPool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        audit_params_0 = mk_audit_params('req-0', 'transaction-0', 'user-0')
        audit_params_1 = mk_audit_params('req-1', 'transaction-1', 'user-0')
        rows = self.successResultOf(pool.query_by_user_id('user-0'))
        assert rows == []

        before = datetime.utcnow()
        self.successResultOf(
            pool._audit_request(audit_params_0, 'req_data_0', 'resp_data_0'))
        self.successResultOf(
            pool._audit_request(audit_params_1, 'req_data_1', 'resp_data_1'))
        self.successResultOf(
            pool._audit_request(mk_audit_params('req-excl'), 'excl', 'excl'))
        after = datetime.utcnow()

        rows = self.successResultOf(pool.query_by_user_id('user-0'))
        created_at_0 = rows[0]['created_at']
        created_at_1 = rows[1]['created_at']
        assert before <= created_at_0 <= created_at_1 <= after

        assert rows == [{
            'request_id': audit_params_0['request_id'],
            'transaction_id': audit_params_0['transaction_id'],
            'user_id': audit_params_0['user_id'],
            'request_data': u'req_data_0',
            'response_data': u'resp_data_0',
            'error': False,
            'created_at': created_at_0,
        }, {
            'request_id': audit_params_1['request_id'],
            'transaction_id': audit_params_1['transaction_id'],
            'user_id': audit_params_1['user_id'],
            'request_data': u'req_data_1',
            'response_data': u'resp_data_1',
            'error': False,
            'created_at': created_at_1,
        }]

    def test_export_all_vouchers(self):
        pool = VoucherPool('testpool', self.conn)
        populate_pool(pool, ['Tank', 'Link'], ['red', 'blue'], [0, 1])
        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 2),
            ('Tank', 'red', False, 2),
        ])

        response = self.successResultOf(
            pool.export_vouchers('req-0', None, None, None))
        assert set(response.keys()) == set(['vouchers', 'warnings'])
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

        self.assert_voucher_counts(pool, [
            ('Link', 'blue', True, 2),
            ('Link', 'red', True, 2),
            ('Tank', 'blue', True, 2),
            ('Tank', 'red', True, 2),
        ])

    def test_export_some_vouchers(self):
        pool = VoucherPool('testpool', self.conn)
        # We give all vouchers of the same type the same voucher code to avoid
        # having to check all the permutations.
        populate_pool(pool, ['Tank', 'Link'], ['red', 'blue'], [0, 0, 0])
        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 3),
            ('Link', 'red', False, 3),
            ('Tank', 'blue', False, 3),
            ('Tank', 'red', False, 3),
        ])

        response = self.successResultOf(pool.export_vouchers(
            'req-0', 1, ['Tank'], ['red']))
        assert set(response.keys()) == set(['vouchers', 'warnings'])
        assert response['warnings'] == []
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'red', 'Tank-red-0'),
        ])
        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 3),
            ('Link', 'red', False, 3),
            ('Tank', 'blue', False, 3),
            ('Tank', 'red', False, 2),
            ('Tank', 'red', True, 1),
        ])

        response = self.successResultOf(pool.export_vouchers(
            'req-1', 1, ['Tank', 'Link'], ['red', 'blue']))
        assert set(response.keys()) == set(['vouchers', 'warnings'])
        assert response['warnings'] == []
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'red', 'Tank-red-0'),
            voucher_dict('Tank', 'blue', 'Tank-blue-0'),
            voucher_dict('Link', 'red', 'Link-red-0'),
            voucher_dict('Link', 'blue', 'Link-blue-0'),
        ])

        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 2),
            ('Tank', 'red', False, 1),
            ('Link', 'blue', True, 1),
            ('Link', 'red', True, 1),
            ('Tank', 'blue', True, 1),
            ('Tank', 'red', True, 2),
        ])

    def test_export_too_many_vouchers(self):
        pool = VoucherPool('testpool', self.conn)
        # We give all vouchers of the same type the same voucher code to avoid
        # having to check all the permutations.
        populate_pool(pool, ['Tank', 'Link'], ['red', 'blue'], [0, 0, 0])
        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 3),
            ('Link', 'red', False, 3),
            ('Tank', 'blue', False, 3),
            ('Tank', 'red', False, 3),
        ])

        response = self.successResultOf(pool.export_vouchers(
            'req-0', 4, ['Tank'], ['red']))
        assert set(response.keys()) == set(['vouchers', 'warnings'])
        assert response['warnings'] == [
            "Insufficient vouchers available for 'Tank' 'red'.",
        ]
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'red', 'Tank-red-0'),
            voucher_dict('Tank', 'red', 'Tank-red-0'),
            voucher_dict('Tank', 'red', 'Tank-red-0'),
        ])
        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 3),
            ('Link', 'red', False, 3),
            ('Tank', 'blue', False, 3),
            ('Tank', 'red', True, 3),
        ])

        response = self.successResultOf(pool.export_vouchers(
            'req-1', 1, ['Tank'], ['red', 'blue']))
        assert set(response.keys()) == set(['vouchers', 'warnings'])
        assert response['warnings'] == [
            "Insufficient vouchers available for 'Tank' 'red'.",
        ]
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'blue', 'Tank-blue-0'),
        ])

        response = self.successResultOf(pool.export_vouchers(
            'req-2', 3, ['Tank', 'Link'], ['red', 'blue']))
        assert set(response.keys()) == set(['vouchers', 'warnings'])
        assert sorted(response['warnings']) == sorted([
            "Insufficient vouchers available for 'Tank' 'red'.",
            "Insufficient vouchers available for 'Tank' 'blue'.",
        ])
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'blue', 'Tank-blue-0'),
            voucher_dict('Tank', 'blue', 'Tank-blue-0'),
            voucher_dict('Link', 'red', 'Link-red-0'),
            voucher_dict('Link', 'red', 'Link-red-0'),
            voucher_dict('Link', 'red', 'Link-red-0'),
            voucher_dict('Link', 'blue', 'Link-blue-0'),
            voucher_dict('Link', 'blue', 'Link-blue-0'),
            voucher_dict('Link', 'blue', 'Link-blue-0'),
        ])

    def test_export_idempotent(self):
        pool = VoucherPool('testpool', self.conn)
        # We give all vouchers of the same type the same voucher code to avoid
        # having to check all the permutations.
        populate_pool(pool, ['Tank', 'Link'], ['red', 'blue'], [0, 0, 0])
        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 3),
            ('Link', 'red', False, 3),
            ('Tank', 'blue', False, 3),
            ('Tank', 'red', False, 3),
        ])

        response = self.successResultOf(pool.export_vouchers(
            'req-0', 1, ['Tank'], ['red']))
        assert set(response.keys()) == set(['vouchers', 'warnings'])
        assert response['warnings'] == []
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'red', 'Tank-red-0'),
        ])
        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 3),
            ('Link', 'red', False, 3),
            ('Tank', 'blue', False, 3),
            ('Tank', 'red', False, 2),
            ('Tank', 'red', True, 1),
        ])

        response = self.successResultOf(pool.export_vouchers(
            'req-0', 1, ['Tank'], ['red']))
        assert set(response.keys()) == set(['vouchers', 'warnings'])
        assert response['warnings'] == []
        assert sorted_dicts(response['vouchers']) == sorted_dicts([
            voucher_dict('Tank', 'red', 'Tank-red-0'),
        ])
        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 3),
            ('Link', 'red', False, 3),
            ('Tank', 'blue', False, 3),
            ('Tank', 'red', False, 2),
            ('Tank', 'red', True, 1),
        ])

        self.failureResultOf(
            pool.export_vouchers('req-0', 1, ['Link'], ['red']), AuditMismatch)
        self.assert_voucher_counts(pool, [
            ('Link', 'blue', False, 3),
            ('Link', 'red', False, 3),
            ('Tank', 'blue', False, 3),
            ('Tank', 'red', False, 2),
            ('Tank', 'red', True, 1),
        ])

from twisted.trial.unittest import TestCase

from airtime_service.models import (
    get_engine, VoucherPool, NoVoucherAvailable, NoVoucherPool)

from .doubles import FakeReactorThreads


class TestVoucherPool(TestCase):
    timeout = 5

    def setUp(self):
        self.engine = get_engine("sqlite://", reactor=FakeReactorThreads())
        self.conn = self.successResultOf(self.engine.connect())
        self._request_count = 0

    def tearDown(self):
        self.successResultOf(self.conn.close())

    def populate_pool(self, pool, operators, denominations, suffixes):
        return self.successResultOf(pool.import_vouchers([
            {
                'operator': operator,
                'denomination': denomination,
                'voucher': '%s-%s-%s' % (operator, denomination, suffix),
            }
            for operator in operators
            for denomination in denominations
            for suffix in suffixes
        ]))

    def assert_voucher_counts(self, pool, expected_rows):
        rows = self.successResultOf(pool.count_vouchers())
        assert sorted(tuple(r) for r in rows) == sorted(expected_rows)

    def _audit_params(self, request_id=None, transaction_id=None,
                      user_id=None):
        if request_id is None:
            request_id = 'req-%s' % (self._request_count,)
            self._request_count += 1
        if transaction_id is None:
            transaction_id = 'tx-%s' % (request_id,)
        if user_id is None:
            user_id = 'user-%s' % (request_id,)
        return {
            'request_id': request_id,
            'transaction_id': transaction_id,
            'user_id': user_id,
        }

    def test_import_creates_table(self):
        pool = VoucherPool('testpool', self.conn)
        f = self.failureResultOf(pool.count_vouchers(), NoVoucherPool)
        assert f.value.args == ('testpool',)
        self.populate_pool(pool, ['Tank'], ['red'], [0])
        self.assert_voucher_counts(pool, [('Tank', 'red', False, 1)])

    def test_exists(self):
        pool = VoucherPool('testpool', self.conn)
        assert not self.successResultOf(pool.exists())
        self.successResultOf(pool.create_tables())
        assert self.successResultOf(pool.exists())

    def test_import_vouchers(self):
        pool = VoucherPool('testpool', self.conn)
        self.successResultOf(pool.create_tables())

        self.successResultOf(pool.import_vouchers([
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

    def test_issue_voucher(self):
        pool = VoucherPool('testpool', self.conn)
        self.populate_pool(pool, ['Tank'], ['red'], [0])
        self.assert_voucher_counts(pool, [('Tank', 'red', False, 1)])

        voucher = self.successResultOf(
            pool.issue_voucher('Tank', 'red', self._audit_params()))
        assert voucher['operator'] == 'Tank'
        assert voucher['denomination'] == 'red'
        assert voucher['voucher'] == 'Tank-red-0'
        self.assert_voucher_counts(pool, [('Tank', 'red', True, 1)])

    def test_issue_voucher_not_available(self):
        pool = VoucherPool('testpool', self.conn)
        self.successResultOf(pool.create_tables())
        self.assert_voucher_counts(pool, [])
        self.failureResultOf(
            pool.issue_voucher('Tank', 'red', self._audit_params()),
            NoVoucherAvailable)

    def test_issue_voucher_idempotent(self):
        pool = VoucherPool('testpool', self.conn)
        self.populate_pool(pool, ['Tank'], ['red'], [0])
        self.assert_voucher_counts(pool, [('Tank', 'red', False, 1)])

        audit_params = self._audit_params()

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

    def test_issue_voucher_idempotent_not_available(self):
        pool = VoucherPool('testpool', self.conn)
        self.successResultOf(pool.create_tables())
        self.assert_voucher_counts(pool, [])

        audit_params = self._audit_params()

        # Issue an unsuccessful request.
        self.failureResultOf(
            pool.issue_voucher('Tank', 'red', audit_params),
            NoVoucherAvailable)

        self.populate_pool(pool, ['Tank'], ['red'], [0])
        self.assert_voucher_counts(pool, [('Tank', 'red', False, 1)])

        # Reissue the same request with available vouchers.
        self.failureResultOf(
            pool.issue_voucher('Tank', 'red', audit_params),
            NoVoucherAvailable)
        self.assert_voucher_counts(pool, [('Tank', 'red', False, 1)])

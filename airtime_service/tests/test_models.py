from twisted.trial.unittest import TestCase

from airtime_service.models import (
    get_engine, VoucherPool, NoVoucherAvailable, NoVoucherPool)

from .doubles import FakeReactorThreads


class TestVoucherPool(TestCase):
    timeout = 5

    def setUp(self):
        self.engine = get_engine("sqlite://", reactor=FakeReactorThreads())
        self.conn = self.successResultOf(self.engine.connect())

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

    def test_import_creates_table(self):
        pool = VoucherPool('testpool', self.conn)
        f = self.failureResultOf(pool.count_vouchers(), NoVoucherPool)
        assert f.value.args == ('testpool',)
        self.populate_pool(pool, ['Tank'], ['red'], [0])
        rows = self.successResultOf(pool.count_vouchers())
        assert rows == [('Tank', 'red', False, 1)]

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
        result = self.successResultOf(pool.count_vouchers())
        assert sorted(tuple(r) for r in result) == [
            ('Link', 'blue', False, 2),
            ('Link', 'red', False, 2),
            ('Tank', 'blue', False, 2),
            ('Tank', 'red', False, 2),
        ]

    def test_issue_voucher(self):
        pool = VoucherPool('testpool', self.conn)
        self.populate_pool(pool, ['Tank'], ['red'], [0])
        rows = self.successResultOf(pool.count_vouchers())
        assert rows == [('Tank', 'red', False, 1)]

        voucher = self.successResultOf(pool.issue_voucher('Tank', 'red'))
        assert voucher['operator'] == 'Tank'
        assert voucher['denomination'] == 'red'
        assert voucher['voucher'] == 'Tank-red-0'

        rows = self.successResultOf(pool.count_vouchers())
        assert rows == [('Tank', 'red', True, 1)]

        self.failureResultOf(
            pool.issue_voucher('Tank', 'red'), NoVoucherAvailable)

from datetime import datetime

from alchimia import TWISTED_STRATEGY

from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, DateTime, Boolean,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import select, func, and_, not_

from twisted.internet.defer import inlineCallbacks, returnValue


class VoucherError(Exception):
    pass


class NoVoucherPool(VoucherError):
    pass


class NoVoucherAvailable(VoucherError):
    pass


def get_engine(conn_str, reactor):
    return create_engine(conn_str, reactor=reactor, strategy=TWISTED_STRATEGY)


class VoucherPool(object):
    def __init__(self, pool_name, connection=None):
        self.name = pool_name
        self._conn = connection
        self.metadata = MetaData()
        self.vouchers = Table(
            self._table_name('vouchers'), self.metadata, *self.voucher_columns)

    @property
    def voucher_columns(self):
        return (
            Column("id", Integer(), primary_key=True),
            Column("operator", String(), nullable=False),
            Column("denomination", String(), nullable=False),
            Column("voucher", String(), nullable=False),
            Column("used", Boolean(), default=False),
            Column("created_at", DateTime(timezone=True)),
            Column("modified_at", DateTime(timezone=True)),
            Column("reason", String(), default=None),
        )

    def _table_name(self, basename):
        return '%s_%s' % (self.name, basename)

    @inlineCallbacks
    def _execute(self, query, *args, **kw):
        try:
            result = yield self._conn.execute(query, *args, **kw)
            returnValue(result)
        except OperationalError as e:
            if 'no such table: ' in str(e):
                raise NoVoucherPool(self.name)
            raise

    @inlineCallbacks
    def _create_table(self, table):
        # This works around alchimia's current inability to create tables only
        # if they don't already exist.
        try:
            yield self._conn.execute(CreateTable(table))
        except OperationalError as e:
            if 'table %s already exists' % (table.name,) not in str(e):
                raise

    def exists(self):
        # It would be nice to make this not use private things.
        return self._conn._engine.has_table(self._table_name('vouchers'))

    @inlineCallbacks
    def create_tables(self):
        yield self._create_table(self.vouchers)

    @inlineCallbacks
    def import_vouchers(self, voucher_dicts):
        # We may not have a table if this is the first import.
        yield self.create_tables()

        # NOTE: We're assuming that this will be fast enough. If it isn't,
        # we'll need to make a database-specific plan of some kind.
        now = datetime.utcnow()
        voucher_rows = [{
            'operator': voucher_dict['operator'],
            'denomination': voucher_dict['denomination'],
            'voucher': voucher_dict['voucher'],
            'created_at': now,
            'modified_at': now,
        } for voucher_dict in voucher_dicts]
        result = yield self._execute(
            self.vouchers.insert(), voucher_rows)
        returnValue(result)

    @inlineCallbacks
    def _get_voucher(self, operator, denomination):
        result = yield self._execute(
            self.vouchers.select().where(and_(
                self.vouchers.c.operator == operator,
                self.vouchers.c.denomination == denomination,
                not_(self.vouchers.c.used),
            )).limit(1))
        voucher = yield result.fetchone()
        returnValue(voucher)

    def _update_voucher(self, voucher_id, **values):
        values['modified_at'] = datetime.utcnow()
        return self._execute(
            self.vouchers.update().where(
                self.vouchers.c.id == voucher_id).values(**values))

    @inlineCallbacks
    def issue_voucher(self, operator, denomination):
        trx = yield self._conn.begin()
        voucher = yield self._get_voucher(operator, denomination)
        if voucher is None:
            raise NoVoucherAvailable()
        yield self._update_voucher(voucher['id'], used=True, reason='issued')
        yield trx.commit()
        returnValue(voucher)

    @inlineCallbacks
    def count_vouchers(self):
        result = yield self._execute(
            select([
                self.vouchers.c.operator,
                self.vouchers.c.denomination,
                self.vouchers.c.used,
                func.count(self.vouchers.c.voucher),
            ]).group_by(
                self.vouchers.c.operator,
                self.vouchers.c.denomination,
                self.vouchers.c.used,
            )
        )
        counts = yield result.fetchall()
        returnValue(counts)

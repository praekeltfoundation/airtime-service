from datetime import datetime
import json

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


class AuditMismatch(VoucherError):
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
        self.audit = Table(
            self._table_name('audit'), self.metadata, *self.audit_columns)
        self.import_audit = Table(
            self._table_name('import_audit'), self.metadata,
            *self.import_audit_columns)
        self.export_audit = Table(
            self._table_name('export_audit'), self.metadata,
            *self.export_audit_columns)
        self.exported_vouchers = Table(
            self._table_name('exported_vouchers'), self.metadata,
            *self.exported_vouchers_columns)

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

    @property
    def audit_columns(self):
        return (
            Column("id", Integer(), primary_key=True),
            Column("request_id", String(), nullable=False, index=True,
                   unique=True),
            Column("transaction_id", String(), nullable=False, index=True),
            Column("user_id", String(), nullable=False, index=True),
            Column("request_data", String(), nullable=False),
            Column("response_data", String(), nullable=False),
            Column("error", Boolean(), nullable=False),
            Column("created_at", DateTime(timezone=True)),
        )

    @property
    def import_audit_columns(self):
        return (
            Column("id", Integer(), primary_key=True),
            Column("request_id", String(), nullable=False, index=True,
                   unique=True),
            Column("content_md5", String(), nullable=False),
            Column("created_at", DateTime(timezone=True)),
        )

    @property
    def export_audit_columns(self):
        return (
            Column("id", Integer(), primary_key=True),
            Column("request_id", String(), nullable=False, index=True,
                   unique=True),
            Column("request_data", String(), nullable=False),
            Column("warnings", String(), nullable=False),
            Column("created_at", DateTime(timezone=True)),
        )

    @property
    def exported_vouchers_columns(self):
        return (
            Column("id", Integer(), primary_key=True),
            Column("request_id", String(), nullable=False, index=True,
                   unique=True),
            Column("voucher_id", Integer(), nullable=False),
            Column("created_at", DateTime(timezone=True)),
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
    def _execute_fetchall(self, query, *args, **kw):
        result = yield self._execute(query, *args, **kw)
        rows = yield result.fetchall()
        returnValue(rows)

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
        trx = yield self._conn.begin()
        yield self._create_table(self.vouchers)
        yield self._create_table(self.audit)
        yield self._create_table(self.import_audit)
        yield self._create_table(self.export_audit)
        yield self._create_table(self.exported_vouchers)
        yield trx.commit()

    def _audit_request(self, audit_params, req_data, resp_data, error=False):
        request_id = audit_params['request_id']
        transaction_id = audit_params['transaction_id']
        user_id = audit_params['user_id']
        return self._execute(
            self.audit.insert().values(**{
                'request_id': request_id,
                'transaction_id': transaction_id,
                'user_id': user_id,
                'request_data': json.dumps(req_data),
                'response_data': json.dumps(resp_data),
                'error': error,
                'created_at': datetime.utcnow(),
            }))

    @inlineCallbacks
    def _get_previous_request(self, audit_params, req_data):
        rows = yield self._execute_fetchall(
            self.audit.select().where(
                self.audit.c.request_id == audit_params['request_id']))
        if not rows:
            returnValue(None)
        [row] = rows
        old_audit_params = {
            'request_id': row['request_id'],
            'transaction_id': row['transaction_id'],
            'user_id': row['user_id'],
        }
        old_req_data = json.loads(row['request_data'])
        old_resp_data = json.loads(row['response_data'])
        if audit_params != old_audit_params or req_data != old_req_data:
            raise AuditMismatch()

        if row['error']:
            exc_class = {
                'no_voucher': NoVoucherAvailable,
            }[old_resp_data]
            raise exc_class()
        returnValue(json.loads(row['response_data']))

    @inlineCallbacks
    def import_vouchers(self, request_id, content_md5, voucher_dicts):
        # We may not have a table if this is the first import.
        yield self.create_tables()

        trx = yield self._conn.begin()

        # Check if we've already done this one.
        rows = yield self._execute_fetchall(
            self.import_audit.select().where(
                self.import_audit.c.request_id == request_id))
        if rows:
            yield trx.rollback()
            [row] = rows
            if row['content_md5'] == content_md5:
                returnValue(None)
            else:
                raise AuditMismatch(row['content_md5'])

        yield self._execute(
            self.import_audit.insert().values(**{
                'request_id': request_id,
                'content_md5': content_md5,
                'created_at': datetime.utcnow(),
            }))

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
        result = yield self._execute(self.vouchers.insert(), voucher_rows)
        yield trx.commit()
        returnValue(result)

    def _format_voucher(self, voucher_row, fields=None):
        if fields is None:
            fields = set(f for f in voucher_row.keys()
                         if f not in ('created_at', 'modified_at'))
        return dict((k, v) for k, v in voucher_row.items() if k in fields)

    @inlineCallbacks
    def _get_voucher(self, operator, denomination):
        result = yield self._execute(
            self.vouchers.select().where(and_(
                self.vouchers.c.operator == operator,
                self.vouchers.c.denomination == denomination,
                not_(self.vouchers.c.used),
            )).limit(1))
        voucher = yield result.fetchone()
        if voucher is not None:
            voucher = self._format_voucher(voucher)
        returnValue(voucher)

    def _update_voucher(self, voucher_id, **values):
        values['modified_at'] = datetime.utcnow()
        return self._execute(
            self.vouchers.update().where(
                self.vouchers.c.id == voucher_id).values(**values))

    @inlineCallbacks
    def _issue_voucher(self, operator, denomination, reason):
        voucher = yield self._get_voucher(operator, denomination)
        if voucher is None:
            returnValue(None)
        yield self._update_voucher(voucher['id'], used=True, reason=reason)
        returnValue(voucher)

    @inlineCallbacks
    def issue_voucher(self, operator, denomination, audit_params):
        audit_req_data = {'operator': operator, 'denomination': denomination}

        # If we have already seen this request, return the same response as
        # before. Appropriate exceptions will be raised here.
        previous_data = yield self._get_previous_request(
            audit_params, audit_req_data)
        if previous_data is not None:
            returnValue(previous_data)

        # This is a new request, so handle it accordingly.
        trx = yield self._conn.begin()
        try:
            voucher = yield self._issue_voucher(
                operator, denomination, 'issued')
            if voucher is None:
                yield self._audit_request(
                    audit_params, audit_req_data, 'no_voucher', error=True)
                raise NoVoucherAvailable()
            else:
                yield self._audit_request(
                    audit_params, audit_req_data, voucher)
        finally:
            yield trx.commit()
        returnValue(voucher)

    def count_vouchers(self):
        return self._execute_fetchall(
            select([
                self.vouchers.c.operator,
                self.vouchers.c.denomination,
                self.vouchers.c.used,
                func.count(self.vouchers.c.voucher).label('count'),
            ]).group_by(
                self.vouchers.c.operator,
                self.vouchers.c.denomination,
                self.vouchers.c.used,
            )
        )

    @inlineCallbacks
    def _query_audit(self, where_clause):
        rows = yield self._execute_fetchall(
            self.audit.select().where(where_clause).order_by(
                self.audit.c.created_at))
        returnValue([{
            'request_id': row['request_id'],
            'transaction_id': row['transaction_id'],
            'user_id': row['user_id'],
            'request_data': json.loads(row['request_data']),
            'response_data': json.loads(row['response_data']),
            'error': row['error'],
            'created_at': row['created_at'],
        } for row in rows])

    def query_by_request_id(self, request_id):
        return self._query_audit(self.audit.c.request_id == request_id)

    def query_by_transaction_id(self, transaction_id):
        return self._query_audit(self.audit.c.transaction_id == transaction_id)

    def query_by_user_id(self, user_id):
        return self._query_audit(self.audit.c.user_id == user_id)

    @inlineCallbacks
    def _list_operators(self):
        rows = yield self._execute_fetchall(
            select([self.vouchers.c.operator]).distinct())
        returnValue([r['operator'] for r in rows])

    @inlineCallbacks
    def _list_denominations(self):
        rows = yield self._execute_fetchall(
            select([self.vouchers.c.denomination]).distinct())
        returnValue([r['denomination'] for r in rows])

    @inlineCallbacks
    def _get_previous_export(self, trx, request_id, request_data):
        # Check if we've already done this one.
        rows = yield self._execute_fetchall(
            self.export_audit.select().where(
                self.export_audit.c.request_id == request_id))
        if not rows:
            returnValue(None)

        [row] = rows
        if json.loads(row['request_data']) != request_data:
            yield trx.rollback()
            raise AuditMismatch(row['request_data'])

        vouchers = yield self._execute_fetchall(
            self.vouchers.select().select_from(
                self.vouchers.join(
                    self.exported_vouchers,
                    self.exported_vouchers.c.voucher_id == self.vouchers.c.id)
                ).where(self.exported_vouchers.c.request_id == request_id))

        yield trx.rollback()
        fields = ['operator', 'denomination', 'voucher']
        returnValue({
            'vouchers': [self._format_voucher(v, fields) for v in vouchers],
            'warnings': json.loads(row['warnings']),
        })

    @inlineCallbacks
    def _export_vouchers(self, request_id, count, operator, denomination):
        vouchers = []
        warnings = []
        while (count is None) or (count > len(vouchers)):
            voucher = yield self._issue_voucher(
                operator, denomination, 'exported')
            if voucher is None:
                break
            yield self._execute(
                self.exported_vouchers.insert().values(**{
                    'request_id': request_id,
                    'voucher_id': voucher['id'],
                }))
            vouchers.append(voucher)

        if (count is not None) and (count > len(vouchers)):
            warnings.append(
                "Insufficient vouchers available for '%s' '%s'." % (
                    operator, denomination))

        returnValue((vouchers, warnings))

    @inlineCallbacks
    def export_vouchers(self, request_id, count, operators, denominations):
        request_data = {
            'count': count,
            'operators': operators,
            'denominations': denominations,
        }
        trx = yield self._conn.begin()

        previous_response = yield self._get_previous_export(
            trx, request_id, request_data)

        if previous_response is not None:
            returnValue(previous_response)

        if operators is None:
            operators = yield self._list_operators()

        if denominations is None:
            denominations = yield self._list_denominations()

        response = {'vouchers': [], 'warnings': []}
        voucher_types = [(operator, denomination)
                         for operator in operators
                         for denomination in denominations]
        for operator, denomination in voucher_types:
            vouchers, warnings = yield self._export_vouchers(
                request_id, count, operator, denomination)
            fields = ['operator', 'denomination', 'voucher']
            response['vouchers'].extend(
                self._format_voucher(voucher, fields) for voucher in vouchers)
            response['warnings'].extend(warnings)

        yield self._execute(
            self.export_audit.insert().values(**{
                'request_id': request_id,
                'request_data': json.dumps(request_data),
                'warnings': json.dumps(response['warnings']),
                'created_at': datetime.utcnow(),
            }))

        yield trx.commit()
        returnValue(response)

from uuid import uuid4


def populate_pool(pool, operators, denominations, suffixes):
    return pool.import_vouchers(str(uuid4()), 'md5', [
        {
            'operator': operator,
            'denomination': denomination,
            'voucher': '%s-%s-%s' % (operator, denomination, suffix),
        }
        for operator in operators
        for denomination in denominations
        for suffix in suffixes
    ])


def mk_audit_params(request_id, transaction_id=None, user_id=None):
    if transaction_id is None:
        transaction_id = 'tx-%s' % (request_id,)
    if user_id is None:
        user_id = 'user-%s' % (transaction_id,)
    return {
        'request_id': request_id,
        'transaction_id': transaction_id,
        'user_id': user_id,
    }


def sorted_dicts(dicts):
    return sorted(dicts, key=lambda d: sorted(d.items()))


def voucher_dict(operator, denomination, voucher, **kw):
    voucher = {
        'operator': operator,
        'denomination': denomination,
        'voucher': voucher,
    }
    voucher.update(kw)
    return voucher

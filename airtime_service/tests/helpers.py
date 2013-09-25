
def populate_pool(pool, operators, denominations, suffixes):
    return pool.import_vouchers([
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

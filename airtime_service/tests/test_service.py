from twisted.python.usage import UsageError
from twisted.trial.unittest import TestCase

from airtime_service import service


class TestService(TestCase):
    def test_make_service(self):
        svc = service.makeService({
            'database-connection-string': 'sqlite://',
            'port': '0',
        })
        assert not svc.running

    def test_make_service_bad_db_conn_str(self):
        self.assertRaises(Exception, service.makeService, {
            'database-connection-string': 'the cloud',
            'port': '0',
        })

    def test_happy_options(self):
        opts = service.Options()
        opts.parseOptions(['-p', '1234', '-d', 'sqlite://'])
        assert set(opts.keys()) == set([
            'port', 'database-connection-string'])
        assert opts['database-connection-string'] == 'sqlite://'
        assert opts['port'] == '1234'

    def test_default_port(self):
        opts = service.Options()
        opts.parseOptions(['-d', 'sqlite://'])
        assert set(opts.keys()) == set([
            'port', 'database-connection-string'])
        assert opts['database-connection-string'] == 'sqlite://'
        assert opts['port'] == '8080'

    def test_db_conn_str_required(self):
        opts = service.Options()
        self.assertRaises(UsageError, opts.parseOptions, [])

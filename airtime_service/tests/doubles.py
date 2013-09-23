from twisted.internet.interfaces import IReactorThreads
from twisted.python.failure import Failure
from zope.interface import implementer


class FakeThreadPool(object):
    def callInThread(self, func, *args, **kw):
        return func(*args, **kw)

    def callInThreadWithCallback(self, onResult, func, *args, **kw):
        if onResult is None:
            onResult = lambda success, result: result
        try:
            result = func(*args, **kw)
        except Exception as e:
            onResult(False, Failure(e))
        else:
            onResult(True, result)


@implementer(IReactorThreads)
class FakeReactorThreads(object):
    def getThreadPool(self):
        return FakeThreadPool()

    def callInThread(self, callable, *args, **kwargs):
        return callable(*args, **kwargs)

    def callFromThread(self, callable, *args, **kw):
        return callable(*args, **kw)

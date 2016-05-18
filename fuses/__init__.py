"""
Circuit breaker pattern implementation
"""

from time import time
from threading import RLock
import functools
import contextlib

__all__ = ("breaker", "Fuses")


def breaker(fuses):
    """
    decorator for circuit breaker pattern
    :param fuses: an instance of Fuses
    """
    def _wrapper(func):
        @functools.wraps(func)
        def __wrapper(*args, **kwargs):
            return fuses.handle(*args, **kwargs)
        return __wrapper
    return _wrapper


@contextlib.contextmanager
def circuit(fuses):
    """
    context for circuit breaker pattern
    :param fuses: an instance of Fuses
    """
    try:
        yield fuses.handle()
    except Exception as exp:
        except_name = exp.__class__.__name__
        if except_name in fuses.exception_list():
            fuses.on_error()
        else:
            raise exp.__class__(exp)
    else:
        fuses.on_success()


class Fuses(object):

    def __init__(self, fails, timeout, exception_list, backoff_cap=30, with_jitter=True):
        self._max_fails = fails
        self._timeout = timeout
        self._exception_list = exception_list
        self._backoff_cap = backoff_cap
        self._with_jitter = with_jitter

        self._last_time = time()
        self._cur_state = BreakerClosedState(self)
        self._lock = RLock()

        self._fail_counter = 0

    def exception_list(self):
        return self._exception_list

    def open(self):
        """convert to `open` state """
        with self._lock:
            self._cur_state = BreakerOpenState(self)

    def close(self):
        """ convert to `closed` state """
        with self._lock:
            self._cur_state = BreakerClosedState(self)

    def half_open(self):
        """ convert to `half-open` state """
        with self._lock:
            self._cur_state = BreakerHalfOpenState(self)

    def incr_counter(self):
        self._fail_counter += 1

    def handle(self):
        with self._lock:
            self._cur_state.handle()

    def on_success(self):
        """ call when success """
        with self._lock:
            self._cur_state.success()

    def on_error(self):
        """ call when error """
        with self._lock:
            self._cur_state.error()


class BreakerState(object):

    def __init__(self, fuses):
        self._fuses = fuses

    def handle(self):
        pass

    def success(self):
        NotImplementedError("Must implement success!")

    def error(self):
        NotImplementedError("Must implement error!")


class BreakerOpenState(BreakerState):

    def __init__(self, fuses):
        super(BreakerOpenState, self).__init__(fuses)

    def success(self):
        pass

    def error(self):
        pass


class BreakerClosedState(BreakerState):

    def __init__(self, fuses):
        super(BreakerClosedState, self).__init__(fuses)

    def success(self):
        pass

    def error(self):
        """ `close` state handle where error"""
        self._fuses.incr_counter()


class BreakerHalfOpenState(BreakerState):

    def __init__(self, fuses):
        super(BreakerHalfOpenState, self).__init__(fuses)

    def success(self):
        pass

    def error(self):
        pass

if __name__ == "__main__":
    f = Fuses(5, 10, [ValueError])
    with circuit(f) as f:
        print 1

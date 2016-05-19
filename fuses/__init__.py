"""
Circuit breaker pattern implementation
"""

from time import time
from threading import RLock
import functools
import contextlib
import random

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
        yield fuses.pre_handle()
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

    @property
    def last_time(self):
        return self._last_time

    @last_time.setter
    def last_time(self, time_):
        self._last_time = time_

    @property
    def reset_timeout(self):
        return self._timeout

    @reset_timeout.setter
    def reset_timeout(self, timeout):
        self._timeout = timeout

    @property
    def backoff_cap(self):
        return self._backoff_cap

    @property
    def with_jitter(self):
        return self._with_jitter

    @property
    def fail_counter(self):
        return self._fail_counter

    def reset_fail_counter(self):
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

    def is_melting_point(self):
        if self._fail_counter > self._max_fails:
            return True
        return False

    def pre_handle(self):
        with self._lock:
            self._cur_state.pre_handle()

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

    def __init__(self, fuses, name):
        self._fuses = fuses
        self._name = name

    @property
    def name(self):
        return self._name

    def pre_handle(self):
        raise NotImplementedError("Must implement pre_handle!")

    def handle(self):
        pass

    def success(self):
        raise NotImplementedError("Must implement success!")

    def error(self):
        raise NotImplementedError("Must implement error!")


class BreakerOpenState(BreakerState):

    def __init__(self, fuses, name='open'):
        super(BreakerOpenState, self).__init__(fuses, name)
        self._fuses.reset_fail_counter()
        self._fuses.last_time = time()

    def pre_handle(self):
        now = time()
        reset_timeout = self._fuses.reset_timeout
        if self._fuses.backoff_cap:
            reset_timeout = self._fuses.reset_timeout * (2 ** self._fuses.fail_counter)
            reset_timeout = min(reset_timeout, self._fuses.backoff_cap)
        if self._fuses.with_jitter:
            reset_timeout = random.random() * reset_timeout
        if now > (self._fuses.last_time + reset_timeout):
            self._fuses.half_open()
        return self.name

    def success(self):
        pass

    def error(self):
        pass


class BreakerClosedState(BreakerState):

    def __init__(self, fuses, name='closed'):
        super(BreakerClosedState, self).__init__(fuses, name)

    def pre_handle(self):
        return self.name

    def success(self):
        pass

    def error(self):
        """ `close` state handle error"""
        if self._fuses.is_melting_point():
            self._fuses.open()
        else:
            self._fuses.incr_counter()


class BreakerHalfOpenState(BreakerState):

    def __init__(self, fuses, name='half_open'):
        super(BreakerHalfOpenState, self).__init__(fuses, name)

    def pre_handle(self):
        return self.name

    def success(self):
        self._fuses.reset_fail_counter()
        self._fuses.close()

    def error(self):
        self._fuses.open()

if __name__ == "__main__":
    f = Fuses(5, 10, [RuntimeError])
    with circuit(f) as f:
        # remote call
        raise RuntimeError

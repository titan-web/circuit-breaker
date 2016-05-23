"""
Circuit breaker pattern implementation
"""

from time import time
from threading import RLock
import random


class FusesManager(object):
    def __init__(self):
        self.circuits = {}

    def get_fuses(self, name, max_fails, timeout, exception_list, back_off_cap=0):
        if name not in self.circuits:
            self.circuits[name] = Fuses(max_fails, timeout, exception_list, back_off_cap)
        return self.circuits[name]


class Fuses(object):
    def __init__(self, fails, timeout, exception_list, back_off_cap=0):
        self._max_fails = fails
        self._timeout = timeout
        self._exception_list = exception_list
        self._back_off_cap = back_off_cap

        self._last_time = time()
        self._cur_state = FusesClosedState(self)
        self._lock = RLock()

        self._fail_counter = 0
        self._try_counter = 0

    @property
    def last_time(self):
        return self._last_time

    @last_time.setter
    def last_time(self, time_):
        self._last_time = time_

    @property
    def cur_state(self):
        return self._cur_state.name

    @property
    def reset_timeout(self):
        return self._timeout

    @reset_timeout.setter
    def reset_timeout(self, timeout):
        self._timeout = timeout

    @property
    def back_off_cap(self):
        return self._back_off_cap

    @property
    def fail_counter(self):
        return self._fail_counter

    @property
    def try_counter(self):
        return self._try_counter

    def reset_fail_counter(self):
        self._fail_counter = 0

    def reset_try_counter(self):
        self._try_counter = 0

    def exception_list(self):
        return self._exception_list

    def open(self):
        """convert to `open` state """
        with self._lock:
            self._cur_state = FusesOpenState(self)

    def close(self):
        """ convert to `closed` state """
        with self._lock:
            self._cur_state = FusesClosedState(self)

    def half_open(self):
        """ convert to `half-open` state """
        with self._lock:
            self._cur_state = FusesHalfOpenState(self)

    def incr_fail_counter(self):
        self._fail_counter += 1

    def incr_try_counter(self):
        self._try_counter += 1

    def is_melting_point(self):
        if self._fail_counter >= self._max_fails:
            return True
        return False

    def pre_handle(self):
        with self._lock:
            return self._cur_state.pre_handle()

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


class FusesState(object):
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


class FusesOpenState(FusesState):
    def __init__(self, fuses, name='open'):
        super(FusesOpenState, self).__init__(fuses, name)
        self._fuses.incr_try_counter()
        self._fuses.last_time = time()

    def pre_handle(self):
        now = time()
        reset_timeout = self._fuses.reset_timeout
        if self._fuses.back_off_cap:
            reset_timeout = self._fuses.reset_timeout * (2 ** self._fuses.try_counter)
            reset_timeout = min(reset_timeout, self._fuses.back_off_cap)
            reset_timeout = random.uniform(0, reset_timeout)
        if now > (self._fuses.last_time + reset_timeout):
            self._fuses.half_open()
        else:
            raise FusesOpenError("fuses open!")
        return self._name

    def success(self):
        pass

    def error(self):
        pass


class FusesClosedState(FusesState):
    def __init__(self, fuses, name='closed'):
        super(FusesClosedState, self).__init__(fuses, name)
        self._fuses.reset_fail_counter()
        self._fuses.reset_try_counter()

    def pre_handle(self):
        return self._name

    def success(self):
        self._fuses.reset_fail_counter()

    def error(self):
        """ `close` state handle error"""
        if self._fuses.is_melting_point():
            self._fuses.open()
            raise FusesOpenError("fuses open!")
        else:
            self._fuses.incr_fail_counter()


class FusesHalfOpenState(FusesState):
    def __init__(self, fuses, name='half_open'):
        super(FusesHalfOpenState, self).__init__(fuses, name)

    def pre_handle(self):
        return self._name

    def success(self):
        self._fuses.close()

    def error(self):
        self._fuses.open()
        raise FusesOpenError("fuses reopen!")


class FusesOpenError(Exception):
    pass

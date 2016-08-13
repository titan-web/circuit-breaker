# coding=utf-8
"""
Circuit breaker pattern implementation
"""

from time import time
from threading import RLock
import random
import os


class FusesManager(object):
    def __init__(self):
        self.circuits = {}

    def get_fuses(self, name, max_fails, timeout, exception_list=None, all_exception=False, back_off_cap=0):
        if name not in self.circuits:
            self.circuits[name] = Fuses(name, max_fails, timeout, exception_list, all_exception, back_off_cap)
        return self.circuits[name]


class Fuses(object):
    def __init__(self, name, fails, timeout, exception_list, all_exception, back_off_cap=0):
        self._name = name
        self._max_fails = fails
        self._timeout = timeout
        self._exception_list = exception_list
        self._all_exception = all_exception
        self._back_off_cap = back_off_cap

        self._last_time = time()
        self._fail_counter = 0
        self._try_counter = 0
        self._cur_state = FusesClosedState(self)
        self._lock = RLock()

    @property
    def last_time(self):
        return self._last_time

    @property
    def name(self):
        return self._name

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
        temp = self._try_counter
        self._try_counter = 0
        return temp

    def get_exception_list(self):
        return self._exception_list

    def is_all_exception(self):
        return self._all_exception

    def open(self):
        with self._lock:
            self._cur_state = FusesOpenState(self)

    def close(self):
        with self._lock:
            self._cur_state = FusesClosedState(self)

    def half_open(self):
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

    def decorators(self, func, *args, **kwargs):
        with self._lock:
            self._cur_state.handle()

    def on_success(self):
        with self._lock:
            self._cur_state.success()

    def on_error(self):
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
    """熔断打开状态"""

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
            raise FusesOpenError(self._fuses.name, self._fuses.cur_state, self._fuses.fail_counter,
                                 self._fuses.try_counter, os.getpid(), reset_timeout)
        return self._name

    def success(self):
        pass

    def error(self):
        pass


class FusesClosedState(FusesState):
    """熔断关闭状态"""

    def __init__(self, fuses, name='closed'):
        super(FusesClosedState, self).__init__(fuses, name)
        self._fuses.reset_fail_counter()
        self.last_try_times = self._fuses.reset_try_counter()

    def pre_handle(self):
        return self._name

    def success(self):
        self._fuses.reset_fail_counter()

    def error(self):
        if self._fuses.is_melting_point():
            self._fuses.open()
            raise FusesOpenError(self._fuses.name, self._fuses.cur_state, self._fuses.fail_counter,
                                 self.last_try_times, os.getpid())
        else:
            self._fuses.incr_fail_counter()


class FusesHalfOpenState(FusesState):
    """`熔断半闭合状态`"""

    def __init__(self, fuses, name='half_open'):
        super(FusesHalfOpenState, self).__init__(fuses, name)

    def pre_handle(self):
        return self._name

    def success(self):
        try_counter = self._fuses.try_counter
        self._fuses.close()
        raise FusesClosedError(self._fuses.name, self._fuses.cur_state, self._fuses.fail_counter,
                               try_counter, os.getpid())

    def error(self):
        self._fuses.open()
        raise FusesOpenError(self._fuses.name, self._fuses.cur_state, self._fuses.fail_counter,
                             self._fuses.try_counter, os.getpid())


class FusesError(Exception):
    def __init__(self, name, state_name, fail_counter, try_counter, pid, reset_timeout=None):
        self._name = name
        self._state_name = state_name
        self._fail_counter = fail_counter
        self._try_counter = try_counter
        self._pid = pid
        self._reset_timeout = reset_timeout

    @property
    def message(self):
        msg_str = "Fuses name:[%s] Pid:[%s] State_name:[%s] Fail_counter:[%s] Try_counter:[%s]" % \
                  (self._name, self._pid, self._state_name, self._fail_counter, self._try_counter)
        if self._reset_timeout:
            msg_str += " Reset_timeout: [%s]" % self._reset_timeout
        return repr(msg_str)

    @property
    def try_counter(self):
        return self._try_counter

    @property
    def fail_counter(self):
        return self._fail_counter

    @property
    def name(self):
        return self._name

    @property
    def pid(self):
        return self._pid


class FusesOpenError(FusesError):

    def is_first(self):
        if self.try_counter > 1:
            return False
        else:
            return True


class FusesClosedError(FusesError):
    pass


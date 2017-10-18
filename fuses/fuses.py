# coding=utf-8
"""
熔断机制
"""

from time import time
import os

from .backoff import ExponentialBackOff


class FusesManager(object):
    """
    熔断实例管理
    """
    def __init__(self):
        self.circuits = {}

    def get_fuses(self, name, max_fails, timeout, exception_list=None, all_exception=False, back_off_cap=0, policy=1):
        if name not in self.circuits:
            self.circuits[name] = Fuses(name, max_fails, timeout, exception_list, all_exception, back_off_cap, policy)
        return self.circuits[name]


class Fuses(object):
    def __init__(self, name, threshold, timeout, exception_list, all_exception, back_off_cap=0, policy=1):
        """

        :param name: 熔断器名称
        :param threshold: 触发熔断阈值
        :param timeout: 二次试探等待时间
        :param exception_list: 触发熔断的异常列表
        :param all_exception: 是否接受所有异常
        :param back_off_cap: 退避算法时间上限
        :param policy: 熔断策略 0=计数法 1=滑动窗口
        """
        self._name = name
        self._threshold = threshold
        self._exception_list = exception_list
        self._all_exception = all_exception
        self._policy = FusesPercentPolicy(threshold) if policy == 1 else FusesCountPolicy(threshold)

        self._last_time = time()
        self._fail_counter = 0
        self._try_counter = 0
        self._request_queue = [1] * 10
        self._cur_state = FusesClosedState(self)
        self.backoff = ExponentialBackOff(interval=timeout, back_off_cap=back_off_cap)

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
    def request_queue(self):
        return self._request_queue

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
        self._cur_state = FusesOpenState(self)

    def close(self):
        self._cur_state = FusesClosedState(self)

    def half_open(self):
        self._cur_state = FusesHalfOpenState(self)

    def append_success_request(self):
        self._request_queue.append(1)
        self._request_queue = self._request_queue[-10:]

    def append_fail_request(self):
        self._request_queue.append(0)
        self._request_queue = self._request_queue[-10:]

    def increase_fail_counter(self):
        self._fail_counter += 1
        self.append_fail_request()

    def increase_try_counter(self):
        self._try_counter += 1

    def is_melting_point(self):
        return self._policy.is_melting_point(self._fail_counter, self._request_queue)

    def pre_handle(self):
        return self._cur_state.pre_handle()

    def on_success(self):
        self._cur_state.success()

    def on_error(self):
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
        self._fuses.last_time = self._fuses.backoff.next_deadline()

    def pre_handle(self):
        if time() > self._fuses.last_time:
            self._fuses.half_open()
        else:
            raise FusesOpenError(self._fuses.name, self._fuses.cur_state, self._fuses.fail_counter,
                                 self._fuses.try_counter, os.getpid(), self._fuses.last_time, self._fuses.request_queue)
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
        self._fuses.reset_try_counter()

    def pre_handle(self):
        return self._name

    def success(self):
        self._fuses.reset_fail_counter()
        self._fuses.append_success_request()

    def error(self):
        self._fuses.increase_fail_counter()
        if self._fuses.is_melting_point():
            self._fuses.open()
            raise FusesOpenError(self._fuses.name, self._fuses.cur_state, self._fuses.fail_counter,
                                 self._fuses.try_counter, os.getpid(), self._fuses.last_time, self._fuses.request_queue)


class FusesHalfOpenState(FusesState):
    """`熔断半闭合状态`"""

    def __init__(self, fuses, name='half_open'):
        super(FusesHalfOpenState, self).__init__(fuses, name)

    def pre_handle(self):
        return self._name

    def success(self):
        try_counter = self._fuses.try_counter
        self._fuses.close()
        self._fuses.append_success_request()
        raise FusesClosedError(self._fuses.name, self._fuses.cur_state, self._fuses.fail_counter,
                               try_counter, os.getpid(), self._fuses.last_time, self._fuses.request_queue)

    def error(self):
        self._fuses.increase_try_counter()
        self._fuses.increase_fail_counter()
        self._fuses.open()
        raise FusesOpenError(self._fuses.name, self._fuses.cur_state, self._fuses.fail_counter,
                             self._fuses.try_counter, os.getpid(), self._fuses.last_time, self._fuses.request_queue)


class FusesError(Exception):
    def __init__(self, name, state_name, fail_counter, try_counter, pid, last_time, requests):
        self._name = name
        self._state_name = state_name
        self._fail_counter = fail_counter
        self._try_counter = try_counter
        self._pid = pid
        self._last_time = last_time
        self._requests = requests

    @property
    def message(self):
        msg_str = "Fuses name:[%s] Pid:[%s] State_name:[%s] Fail_counter:[%s] Try_counter:[%s] Last_time:[%s] " \
                  "Requests:[%s]" % \
                  (self._name, self._pid, self._state_name, self._fail_counter, self._try_counter,
                   self._last_time, self._requests)
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


class FusesPolicyBase(object):
    """
    熔断策略
    """
    def __init__(self, threshold):
        self.threshold = threshold

    def is_melting_point(self, fail_counter, requests):
        raise NotImplementedError("Must implement error!")


class FusesCountPolicy(FusesPolicyBase):
    def __init__(self, threshold):
        super(FusesCountPolicy, self).__init__(threshold)

    def is_melting_point(self, fail_counter, requests):
        if fail_counter >= self.threshold:
            return True
        return False


class FusesPercentPolicy(FusesPolicyBase):
    def __init__(self, threshold):
        super(FusesPercentPolicy, self).__init__(threshold)

    def is_melting_point(self, fail_counter, requests):
        if not requests:
            return False
        if requests[-1] == 0 and (sum(requests) <= len(requests) - self.threshold):
            return True
        return False

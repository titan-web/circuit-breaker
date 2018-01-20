# coding=utf-8
import random
from time import time


class ExponentialBackOff(object):

    def __init__(self,
                 interval=5,
                 factor=0.5,
                 back_off_cap=60,
                 multiplier=1.5):

        self.started = None
        self.multiplier = multiplier
        self.max_interval = int(back_off_cap * 1000) if back_off_cap else int(interval * 1000)
        self.factor = min(max(factor, 0), 1)
        self.interval = int(interval * 1000)
        self.current_interval = self.interval

    def next_deadline(self):
        self.started = time()
        return self.started + self.back_off_time()

    def reset(self):
        self.current_interval = self.interval

    def back_off_time(self):
        interval = self.__get_random_interval()

        self.__increase_interval()

        return round(interval / 1000, 2)

    def __increase_interval(self):
        if self.current_interval >= (self.max_interval / self.multiplier):
            self.current_interval = self.max_interval
        else:
            self.current_interval = self.current_interval * self.multiplier

    def __get_random_interval(self):
        rand = random.random()
        delta = self.factor * rand

        min_interval = self.current_interval - delta
        max_interval = self.current_interval + delta

        return int(min_interval + (rand * (max_interval - min_interval + 1)))

#!/usr/bin/env python
# -*- coding: utf-8 -*-


class FusesManagerConfig(object):

    DICT = [

        {
            "name": "test",
            "path": "/ws/test",
            "max_fails": 5,
            "timeout": 10,
            "exception_list": [],
            "all_exception": 1,
            "back_off_cap": 60
        }

    ]

    @classmethod
    def get_conf_by_path(cls, path):
        for item in cls.DICT:
            if item['path'] == path:
                return item

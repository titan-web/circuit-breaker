#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urlparse import urlparse


class FusesManagerConfig(object):

    DICT = [

        {
            "name": "test",
            "path": "/ws/test",
            "host": "www.example.com",
            "max_fails": 5,
            "timeout": 10,
            "exception_list": [],
            "all_exception": 1,
            "back_off_cap": 60,
            "policy": 1
        }

    ]

    @classmethod
    def get_conf_by_path(cls, path):
        for item in cls.DICT:
            if item['path'] == path:
                return item

    @classmethod
    def get_conf_by_url(cls, url):
        url_info = urlparse(url)
        path = url_info.path.strip("/")
        for item in cls.DICT:
            if item.get('host', '') == url_info.netloc and re.match(item['path'], path):
                return item
            if item['path'] == path:
                return item

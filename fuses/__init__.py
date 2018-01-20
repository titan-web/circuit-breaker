# coding=utf-8
"""
Circuit breaker pattern implementation

Example:
    >>> fuses_manage = FusesManager()
    >>> url = "http://www.example.com"
    >>> try:
    >>>     with circuit(fuses_manage, url) as a:
    >>>         # remote call raise error
    >>>         raise Exception("self read timeout!")
    >>>
    >>> except FusesOpenError as exp:
    >>>     # do what you want
    >>>     print exp

"""

import contextlib

from django.conf import settings

from .utils import import_module
from .fuses import FusesManager, FusesOpenError, Fuses, FusesClosedError, FusesHalfOpenError

__all__ = ("circuit", "FusesOpenError", "FusesClosedError", "FusesHalfOpenError", "fuses_manage", "ExponentialBackOff")
__version__ = "1.0.0"


@contextlib.contextmanager
def circuit(fuses_manager, url):
    """
    context for circuit breaker pattern
    :param fuses_manager: single instance of FusesManager
    :param url: http url
    """
    fuse = get_fuse_instance(fuses_manager, url)
    if fuse:
        fuse.pre_handle()
        try:
            yield fuse.name
        except Exception as exp:
            except_class = exp.__class__
            is_all_exception = fuse.is_all_exception()
            if except_class.__name__ in fuse.get_exception_list() or \
                    (is_all_exception and except_class.__name__ != "FusesOpenError"):
                fuse.on_error()
            else:
                raise exp
        else:
            fuse.on_success()
    # 没有熔断器配置则跳过熔断机制
    else:
        yield


def get_fuse_instance(fuses_manager, url):
    """获取熔断器实例"""
    config = get_uri_config(url)
    if not config:
        return None
    fuse = fuses_manager.get_fuses(name=config.get("name"), max_fails=config.get("max_fails"),
                                   timeout=config.get("timeout"), exception_list=config.get("exception_list"),
                                   all_exception=config.get("all_exception"), back_off_cap=config.get("back_off_cap"),
                                   policy=config.get("policy"))
    return fuse


def get_uri_config(url):
    """获取PATH对应的配置
    """
    fuses_conf_path = getattr(settings, "FUSES_MANAGER_CONF", "")
    if not fuses_conf_path:
        return None
    try:
        mod_path, cls_name = fuses_conf_path.rsplit('.', 1)
        mod = import_module(mod_path)
        manager_conf = getattr(mod, cls_name)
        config = getattr(manager_conf, "get_conf_by_url")(url)
    except (AttributeError, ImportError, ValueError):
        return None

    return config


fuses_manage = FusesManager()

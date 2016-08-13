# coding=utf-8
"""
Circuit breaker pattern implementation

Example:
    >>> fuses_manage = FusesManager()
    >>> url = "http://google.com"
    >>> try:
    >>>     with circuit(fuses_manage, url) as a:
    >>>         # remote call raise error
    >>>         raise Exception("self read timeout!")
    >>>
    >>> except FusesOpenError as exp:
    >>>     # do what you want
    >>>     print exp

"""

import functools
import contextlib
from urlparse import urlparse
from fuses_manager_conf import FusesManagerConfig
from .fuses import FusesManager, FusesOpenError, Fuses, FusesClosedError

__all__ = ("circuit", "FusesOpenError", "FusesClosedError", "fuses_manage")
__version__ = "1.0.0"


def breaker(fuses_manager):
    """
    decorator for circuit breaker pattern
    :param fuses_manager: an instance of FusesManager
    """

    def _wrapper(func):
        @functools.wraps(func)
        def __wrapper(*args, **kwargs):
            fuse = get_fuse_instance(fuses_manager, "default")
            return fuse.decorators(func, *args, **kwargs)

        return __wrapper

    return _wrapper


@contextlib.contextmanager
def circuit(fuses_manager, url):
    """
    context for circuit breaker pattern
    :param fuses_manager: single instance of FusesManager
    :param url: http url
    """
    fuse = get_fuse_instance(fuses_manager, url)
    if fuse:
        try:

            yield fuse.pre_handle()
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
    path = urlparse(url).path
    config = get_uri_config(path)
    if not config:
        return None
    fuse = fuses_manager.get_fuses(name=config.get("name"), max_fails=config.get("max_fails"),
                                   timeout=config.get("timeout"), exception_list=config.get("exception_list"),
                                   all_exception=config.get("all_exception"), back_off_cap=config.get("back_off_cap"))
    return fuse


def get_uri_config(path):
    """获取PATH对应的配置
    """
    path = path.strip("/")

    try:
        config = getattr(FusesManagerConfig, "get_conf_by_path")(path)
    except (AttributeError, ImportError, ValueError):
        return None

    return config


fuses_manage = FusesManager()

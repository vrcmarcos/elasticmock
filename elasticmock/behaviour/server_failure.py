# -*- coding: utf-8 -*-

from functools import wraps

__ENABLED = False


def enable():
    global __ENABLED
    __ENABLED = True


def disable():
    global __ENABLED
    __ENABLED = False


def server_failure(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if __ENABLED:
            response = {
                'status_code': 500,
                'error': 'Internal Server Error'
            }
        else:
            response = f(*args, **kwargs)
        return response
    return decorated

# -*- coding: utf-8 -*-

from elasticmock.behaviour import server_failure


def disable_all():
    server_failure.disable()

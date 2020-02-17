# -*- coding: utf-8 -*-

from abc import ABCMeta

from elasticmock import behaviour
from tests import TestElasticmock


class TestElasticmockBehaviour(TestElasticmock, metaclass=ABCMeta):

    def tearDown(self):
        behaviour.disable_all()

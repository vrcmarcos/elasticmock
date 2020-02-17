# -*- coding: utf-8 -*-

from elasticmock import behaviour
from tests import TestElasticmock


class TestElasticmockBehaviour(TestElasticmock):

    def tearDown(self):
        behaviour.disable_all()

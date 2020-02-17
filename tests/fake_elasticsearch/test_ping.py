# -*- coding: utf-8 -*-

from tests import TestElasticmock


class TestPing(TestElasticmock):

    def test_should_return_true_when_ping(self):
        self.assertTrue(self.es.ping())

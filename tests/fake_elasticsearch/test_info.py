# -*- coding: utf-8 -*-

from tests import TestElasticmock


class TestInfo(TestElasticmock):

    def test_should_return_status_200_for_info(self):
        info = self.es.info()
        self.assertEqual(info.get('status'), 200)

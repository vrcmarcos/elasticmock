# -*- coding: utf-8 -*-

from tests import TestElasticmock, INDEX_NAME


class TestRefresh(TestElasticmock):

    def test_should_refresh_index(self):
        self.es.indices.create(INDEX_NAME)
        self.es.indices.refresh(INDEX_NAME)
        self.assertTrue(self.es.indices.exists(INDEX_NAME))

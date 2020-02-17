# -*- coding: utf-8 -*-

from tests import TestElasticmock, INDEX_NAME


class TestDelete(TestElasticmock):

    def test_should_delete_index(self):
        self.assertFalse(self.es.indices.exists(INDEX_NAME))

        self.es.indices.create(INDEX_NAME)
        self.assertTrue(self.es.indices.exists(INDEX_NAME))

        self.es.indices.delete(INDEX_NAME)
        self.assertFalse(self.es.indices.exists(INDEX_NAME))

    def test_should_delete_inexistent_index(self):
        self.assertFalse(self.es.indices.exists(INDEX_NAME))

        self.es.indices.delete(INDEX_NAME)
        self.assertFalse(self.es.indices.exists(INDEX_NAME))

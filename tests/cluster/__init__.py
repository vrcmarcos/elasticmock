# -*- coding: utf-8 -*-

import unittest

import elasticsearch

from elasticmock import elasticmock

INDEX_NAME = 'test_index'


class TestElasticmock(unittest.TestCase):

    @elasticmock
    def setUp(self):
        self.es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

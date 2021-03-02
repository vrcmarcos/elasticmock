# -*- coding: utf-8 -*-

import unittest
from datetime import datetime

import elasticsearch

from elasticmock import elasticmock

INDEX_NAME = 'test_index'
DOC_TYPE = 'doc-Type'
DOC_ID = 'doc-id'
BODY = {
    'author': 'kimchy',
    'text': 'Elasticsearch: cool. bonsai cool.',
    'timestamp': datetime.now(),
}


class TestElasticmock(unittest.TestCase):

    @elasticmock
    def setUp(self):
        self.es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

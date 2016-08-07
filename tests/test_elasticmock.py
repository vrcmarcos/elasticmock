# -*- coding: utf-8 -*-

from unittest import TestCase

import elasticsearch
from mock import patch

from elasticmock import get_elasticmock
from elasticmock.fake_elasticsearch import FakeElasticsearch


class TestFakeElasticsearch(TestCase):

    @patch('elasticsearch.Elasticsearch', get_elasticmock)
    def setUp(self):
        self.es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

    def test_should_create_fakeelasticsearch_instance(self):
        self.assertIsInstance(self.es, FakeElasticsearch)

    def test_should_index_document(self):
        index_name = 'test_index'
        doc_type = 'doc-Type'
        body = '{"string": "content", "id": 1}'
        data = self.es.index(index=index_name, doc_type=doc_type, body=body)

        self.assertEqual(doc_type, data.get('_type'))
        self.assertTrue(data.get('created'))
        self.assertEqual(1, data.get('_version'))
        self.assertEqual(index_name, data.get('_index'))

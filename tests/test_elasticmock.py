# -*- coding: utf-8 -*-

from unittest import TestCase

import elasticsearch
from mock import patch

from elasticmock import get_elasticmock
from elasticmock.fake_elasticsearch import FakeElasticsearch
from elasticsearch.exceptions import NotFoundError


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

    def test_should_raise_notfounderror_when_nonindexed_id_is_used(self):
        index_name = 'test_index'

        with self.assertRaises(NotFoundError):
            self.es.get(index=index_name, id='1')

    def test_should_get_document_with_id(self):
        index_name = 'test_index'
        doc_type = 'doc-Type'
        body = '{"string": "content", "id": 1}'
        data = self.es.index(index=index_name, doc_type=doc_type, body=body)

        document_id = data.get('_id')
        target_doc = self.es.get(index=index_name, id=document_id)

        expected = {
            '_type': doc_type,
            '_source': body,
            '_index': index_name,
            '_version': 1,
            'found': True,
            '_id': document_id
        }

        self.assertDictEqual(expected, target_doc)

    def test_should_get_document_with_id_and_doc_type(self):
        index_name = 'test_index'
        doc_type = 'doc-Type'
        body = '{"string": "content", "id": 1}'
        data = self.es.index(index=index_name, doc_type=doc_type, body=body)

        document_id = data.get('_id')
        target_doc = self.es.get(index=index_name, id=document_id, doc_type=doc_type)

        expected = {
            '_type': doc_type,
            '_source': body,
            '_index': index_name,
            '_version': 1,
            'found': True,
            '_id': document_id
        }

        self.assertDictEqual(expected, target_doc)

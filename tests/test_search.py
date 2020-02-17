# -*- coding: utf-8 -*-

from elasticsearch.exceptions import NotFoundError

from tests import TestElasticmock, INDEX_NAME, DOC_TYPE


class TestSearch(TestElasticmock):

    def test_should_raise_notfounderror_when_search_for_unexistent_index(self):
        with self.assertRaises(NotFoundError):
            self.es.search(index=INDEX_NAME)

    def test_should_return_hits_hits_even_when_no_result(self):
        search = self.es.search()
        self.assertEqual(0, search.get('hits').get('total'))
        self.assertListEqual([], search.get('hits').get('hits'))

    def test_should_return_all_documents(self):
        index_quantity = 10
        for i in range(0, index_quantity):
            self.es.index(index='index_{0}'.format(i), doc_type=DOC_TYPE, body={'data': 'test_{0}'.format(i)})

        search = self.es.search()
        self.assertEqual(index_quantity, search.get('hits').get('total'))

    def test_should_return_only_indexed_documents_on_index(self):
        index_quantity = 2
        for i in range(0, index_quantity):
            self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body={'data': 'test_{0}'.format(i)})

        search = self.es.search(index=INDEX_NAME)
        self.assertEqual(index_quantity, search.get('hits').get('total'))

    def test_should_return_only_indexed_documents_on_index_with_doc_type(self):
        index_quantity = 2
        for i in range(0, index_quantity):
            self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body={'data': 'test_{0}'.format(i)})
        self.es.index(index=INDEX_NAME, doc_type='another-Doctype', body={'data': 'test'})

        search = self.es.search(index=INDEX_NAME, doc_type=DOC_TYPE)
        self.assertEqual(index_quantity, search.get('hits').get('total'))

    def test_should_search_in_multiple_indexes(self):
        self.es.index(index='groups', doc_type='groups', body={'budget': 1000})
        self.es.index(index='users', doc_type='users', body={'name': 'toto'})
        self.es.index(index='pcs', doc_type='pcs', body={'model': 'macbook'})

        result = self.es.search(index=['users', 'pcs'])
        self.assertEqual(2, result.get('hits').get('total'))

    def test_usage_of_aggregations(self):
        self.es.index(index='index', doc_type='document', body={'genre': 'rock'})

        body = {"aggs": {"genres": {"terms": {"field": "genre"}}}}
        result = self.es.search(index='index', body=body)

        self.assertTrue('aggregations' in result)

    def test_search_with_scroll_param(self):
        for _ in range(100):
            self.es.index(index='groups', doc_type='groups', body={'budget': 1000})

        result = self.es.search(index='groups', params={'scroll': '1m', 'size': 30})
        self.assertNotEqual(None, result.get('_scroll_id', None))
        self.assertEqual(30, len(result.get('hits').get('hits')))
        self.assertEqual(100, result.get('hits').get('total'))

# -*- coding: utf-8 -*-

from tests import TestElasticmock, DOC_TYPE


class TestCount(TestElasticmock):

    def test_should_return_count_for_indexed_documents_on_index(self):
        index_quantity = 0
        for i in range(0, index_quantity):
            self.es.index(index='index_{0}'.format(i), doc_type=DOC_TYPE, body={'data': 'test_{0}'.format(i)})

        count = self.es.count()
        self.assertEqual(index_quantity, count.get('count'))

    def test_should_count_in_multiple_indexes(self):
        self.es.index(index='groups', doc_type='groups', body={'budget': 1000})
        self.es.index(index='users', doc_type='users', body={'name': 'toto'})
        self.es.index(index='pcs', doc_type='pcs', body={'model': 'macbook'})

        result = self.es.count(index=['users', 'pcs'])
        self.assertEqual(2, result.get('count'))

    def test_should_count_with_empty_doc_types(self):
        self.es.index(index='index', doc_type=DOC_TYPE, body={'data': 'test'})
        count = self.es.count(doc_type=[])
        self.assertEqual(1, count.get('count'))

    def test_should_return_skipped_shards(self):
        self.es.index(index='index', doc_type=DOC_TYPE, body={'data': 'test'})
        count = self.es.count(doc_type=[])
        self.assertEqual(0, count.get('_shards').get('skipped'))

    def test_should_count_with_doc_types(self):
        self.es.index(index='index', doc_type=DOC_TYPE, body={'data': 'test1'})
        self.es.index(index='index', doc_type='different-doc-type', body={'data': 'test2'})
        count = self.es.count(doc_type=DOC_TYPE)
        self.assertEqual(1, count.get('count'))

# -*- coding: utf-8 -*-
import datetime

from elasticsearch.exceptions import NotFoundError
from parameterized import parameterized

from tests import TestElasticmock, INDEX_NAME, DOC_TYPE


class TestSearch(TestElasticmock):

    def test_should_raise_notfounderror_when_search_for_unexistent_index(self):
        with self.assertRaises(NotFoundError):
            self.es.search(index=INDEX_NAME)

    def test_should_return_hits_hits_even_when_no_result(self):
        search = self.es.search()
        self.assertEqual(0, search.get('hits').get('total'))
        self.assertListEqual([], search.get('hits').get('hits'))

    def test_should_return_skipped_shards(self):
        search = self.es.search()
        self.assertEqual(0, search.get('_shards').get('skipped'))

    def test_should_return_all_documents(self):
        index_quantity = 10
        for i in range(0, index_quantity):
            self.es.index(index='index_{0}'.format(i), doc_type=DOC_TYPE, body={'data': 'test_{0}'.format(i)})

        search = self.es.search()
        self.assertEqual(index_quantity, search.get('hits').get('total'))

    def test_should_return_all_documents_match_all(self):
        index_quantity = 10
        for i in range(0, index_quantity):
            self.es.index(index='index_{0}'.format(i), doc_type=DOC_TYPE, body={'data': 'test_{0}'.format(i)})

        search = self.es.search(body={'query': {'match_all': {}}})
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

    def test_search_with_match_query(self):
        for i in range(0, 10):
            self.es.index(index='index_for_search', doc_type=DOC_TYPE, body={'data': 'test_{0}'.format(i)})

        response = self.es.search(index='index_for_search', doc_type=DOC_TYPE,
                                  body={'query': {'match': {'data': 'TEST'}}})
        self.assertEqual(response['hits']['total'], 10)
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 10)

        response = self.es.search(index='index_for_search', doc_type=DOC_TYPE, body={'query': {'match': {'data': '3'}}})
        self.assertEqual(response['hits']['total'], 1)
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]['_source'], {'data': 'test_3'})

    def test_search_with_match_query_in_int_list(self):
        for i in range(0, 10):
            self.es.index(index='index_for_search', doc_type=DOC_TYPE, body={'data': [i, 11, 13]})
        response = self.es.search(index='index_for_search', doc_type=DOC_TYPE, body={'query': {'match': {'data': 1}}})
        self.assertEqual(response['hits']['total'], 1)
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]['_source'], {'data': [1, 11, 13]})

    def test_search_with_match_query_in_string_list(self):
        for i in range(0, 10):
            self.es.index(index='index_for_search', doc_type=DOC_TYPE, body={'data': [str(i), 'two', 'three']})

        response = self.es.search(index='index_for_search', doc_type=DOC_TYPE, body={'query': {'match': {'data': '1'}}})
        self.assertEqual(response['hits']['total'], 1)
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]['_source'], {'data': ['1', 'two', 'three']})

    def test_search_with_term_query(self):
        for i in range(0, 10):
            self.es.index(index='index_for_search', doc_type=DOC_TYPE, body={'data': 'test_{0}'.format(i)})

        response = self.es.search(index='index_for_search', doc_type=DOC_TYPE,
                                  body={'query': {'term': {'data': 'TEST'}}})
        self.assertEqual(response['hits']['total'], 0)
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 0)

        response = self.es.search(index='index_for_search', doc_type=DOC_TYPE, body={'query': {'term': {'data': '3'}}})
        self.assertEqual(response['hits']['total'], 1)
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]['_source'], {'data': 'test_3'})

    def test_search_with_bool_query(self):
        for i in range(0, 10):
            self.es.index(index='index_for_search', doc_type=DOC_TYPE, body={'id': i})

        response = self.es.search(index='index_for_search', doc_type=DOC_TYPE,
                                  body={'query': {'bool': {'filter': [{'term': {'id': 1}}]}}})
        self.assertEqual(response['hits']['total'], 1)
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 1)

    def test_search_with_terms_query(self):
        for i in range(0, 10):
            self.es.index(index='index_for_search', doc_type=DOC_TYPE, body={'id': i})

        response = self.es.search(index='index_for_search', doc_type=DOC_TYPE,
                                  body={'query': {'terms': {'id': [1, 2, 3]}}})
        self.assertEqual(response['hits']['total'], 3)
        hits = response['hits']['hits']
        self.assertEqual(len(hits), 3)

    def test_query_on_nested_data(self):
        for i, y in enumerate(['yes', 'no']):
            self.es.index('index_for_search', doc_type=DOC_TYPE,
                          body={'id': i, 'data': {'x': i, 'y': y}})

        for term, value, i in [('data.x', 1, 1), ('data.y', 'yes', 0)]:
            response = self.es.search(index='index_for_search', doc_type=DOC_TYPE,
                                      body={'query': {'term': {term: value}}})
            self.assertEqual(1, response['hits']['total'])
            doc = response['hits']['hits'][0]['_source']
            self.assertEqual(i, doc['id'])

    @parameterized.expand(
        [
            (
                    'timestamp gt',
                    {'timestamp': {'gt': datetime.datetime(2009, 1, 1, 10, 20, 0).isoformat()}},
                    range(5, 12),
            ),
            (
                    'timestamp gte',
                    {'timestamp': {'gte': datetime.datetime(2009, 1, 1, 10, 20, 0).isoformat()}},
                    range(4, 12),
            ),
            (
                    'timestamp lt',
                    {'timestamp': {'lt': datetime.datetime(2009, 1, 1, 10, 35, 0).isoformat()}},
                    range(7),
            ),
            (
                    'timestamp lte',
                    {'timestamp': {'lte': datetime.datetime(2009, 1, 1, 10, 35, 0).isoformat()}},
                    range(8),
            ),
            (
                    'timestamp combination',
                    {
                        'timestamp': {
                            'gt': datetime.datetime(2009, 1, 1, 10, 15, 0).isoformat(),
                            'lte': datetime.datetime(2009, 1, 1, 10, 35, 0).isoformat(),
                        }
                    },
                    range(4, 8),
            ),
            (
                    'data_int gt',
                    {'data_int': {'gt': 40}},
                    range(5, 12),
            ),
            (
                    'data_int gte',
                    {'data_int': {'gte': 40}},
                    range(4, 12),
            ),
            (
                    'data_int lt',
                    {'data_int': {'lt': 70}},
                    range(7),
            ),
            (
                    'data_int lte',
                    {'data_int': {'lte': 70}},
                    range(8),
            ),
            (
                    'data_int combination',
                    {'data_int': {'gt': 30, 'lte': 70}},
                    range(4, 8),
            ),
        ]
    )
    def test_search_with_range_query(self, _, query_range, expected_ids):
        for i in range(0, 12):
            body = {
                'id': i,
                'timestamp': datetime.datetime(2009, 1, 1, 10, 5 * i, 0),
                'data_int': 10 * i,
            }
            self.es.index(index='index_for_search', doc_type=DOC_TYPE, body=body)

        response = self.es.search(
            index='index_for_search',
            doc_type=DOC_TYPE,
            body={'query': {'range': query_range}},
        )

        self.assertEqual(len(expected_ids), response['hits']['total'])
        hits = response['hits']['hits']
        self.assertEqual(set(expected_ids), set(hit['_source']['id'] for hit in hits))

    def test_bucket_aggregation(self):
        data = [
            {"data_x": 1, "data_y": "a"},
            {"data_x": 1, "data_y": "a"},
            {"data_x": 2, "data_y": "a"},
            {"data_x": 2, "data_y": "b"},
            {"data_x": 3, "data_y": "b"},
        ]
        for body in data:
            self.es.index(index='index_for_search', doc_type=DOC_TYPE, body=body)

        response = self.es.search(
            index="index_for_search",
            doc_type=DOC_TYPE,
            body={
                "query": {"match_all": {}},
                "aggs": {
                    "stats": {
                        "composite": {
                            "sources": [{"data_x": {"terms": {"field": "data_x"}}}],
                            "size": 10000,
                        },
                        "aggs": {
                            "distinct_data_y": {"cardinality": {"field": "data_y"}}
                        },
                    }
                },
            },
        )

        expected = [
            {"key": {"data_x": 1}, "doc_count": 2},
            {"key": {"data_x": 2}, "doc_count": 2},
            {"key": {"data_x": 3}, "doc_count": 1},
        ]
        actual = response["aggregations"]["stats"]["buckets"]

        for x, y in zip(expected, actual):
            self.assertDictEqual(x["key"], y["key"])
            self.assertEqual(x["doc_count"], y["doc_count"])

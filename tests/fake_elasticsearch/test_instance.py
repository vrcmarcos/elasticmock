# -*- coding: utf-8 -*-

import elasticsearch

from elasticmock import elasticmock
from elasticmock.fake_elasticsearch import FakeElasticsearch
from tests import TestElasticmock


class TestInstance(TestElasticmock):

    def test_should_create_fake_elasticsearch_instance(self):
        self.assertIsInstance(self.es, FakeElasticsearch)

    @elasticmock
    def test_should_return_same_elastic_instance_when_instantiate_more_than_one_instance_with_same_host(self):
        es1 = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
        es2 = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
        self.assertEqual(es1, es2)

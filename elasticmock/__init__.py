# -*- coding: utf-8 -*-

from functools import wraps

from mock import patch

from elasticmock.fake_elasticsearch import FakeElasticsearch

ELASTIC_INSTANCES = {}


def _get_elasticmock(hosts=None):
    elastic_key = 'localhost:9200' if hosts is None else '{0}:{1}'.format(hosts[0].get('host'), hosts[0].get('port'))
    if elastic_key in ELASTIC_INSTANCES:
        connection = ELASTIC_INSTANCES.get(elastic_key)
    else:
        connection = FakeElasticsearch()
        ELASTIC_INSTANCES[elastic_key] = connection
    return connection


def _clear_instances():
    ELASTIC_INSTANCES.clear()


def elasticmock(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        with patch('elasticsearch.Elasticsearch', _get_elasticmock):
            result = f(*args, **kwargs)
            _clear_instances()
        return result
    return decorated

# -*- coding: utf-8 -*-

from functools import wraps

from elasticsearch.client import _normalize_hosts
from mock import patch

from elasticmock.fake_elasticsearch import FakeElasticsearch

ELASTIC_INSTANCES = {}


def _get_elasticmock(hosts=None, **kwargs):
    import ipdb; ipdb.set_trace()
    # host = _normalize_hosts(hosts)[0]
    host = {}
    elastic_key = '{0}:{1}'.format(
        host.get('host', 'localhost'), host.get('port', 9200)
    )

    if elastic_key in ELASTIC_INSTANCES:
        connection = ELASTIC_INSTANCES.get(elastic_key)
    else:
        connection = FakeElasticsearch()
        ELASTIC_INSTANCES[elastic_key] = connection
    return connection


def elasticmock(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # ELASTIC_INSTANCES.clear()
        # with patch('elasticsearch.Elasticsearch', FakeElasticsearch):
        with patch('elasticsearch.client.Elasticsearch.__init__', FakeElasticsearch.__init__):
            result = f(*args, **kwargs)
        return result
    return decorated

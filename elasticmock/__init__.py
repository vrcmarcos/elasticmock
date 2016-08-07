# -*- coding: utf-8 -*-

from elasticsearch import Transport

from elasticmock.fake_elasticsearch import FakeElasticsearch


def get_elasticmock(hosts=None, transport_class=Transport, **kwargs):
    return FakeElasticsearch(hosts=hosts, transport_class=transport_class, **kwargs)

# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch, Transport
from elasticmock.utilities import get_random_id


class FakeElasticsearch(Elasticsearch):
    __data_dict = None

    def __init__(self, hosts=None, transport_class=Transport, **kwargs):
        self.__data_dict = {}

    def index(self, index, doc_type, body, id=None, params=None):
        if index not in self.__data_dict:
            self.__data_dict[index] = {}

        document_id = get_random_id()
        self.__data_dict[index] = {
            '_type': doc_type,
            '_id': document_id,
            '_source': body
        }

        return {
            '_type': doc_type,
            '_id': document_id,
            'created': True,
            '_version': 1,
            '_index': index
        }

# -*- coding: utf-8 -*-

import json

from elasticsearch import Elasticsearch, Transport
from elasticsearch.exceptions import NotFoundError

from elasticmock.utilities import get_random_id


class FakeElasticsearch(Elasticsearch):
    __data_dict = None

    def __init__(self, hosts=None, transport_class=Transport, **kwargs):
        self.__data_dict = {}

    def index(self, index, doc_type, body, document_id=None, params=None):
        if index not in self.__data_dict:
            self.__data_dict[index] = list()

        if document_id is None:
            document_id = get_random_id()

        version = 1

        self.__data_dict[index].append({
            '_type': doc_type,
            '_id': document_id,
            '_source': body,
            '_index': index,
            '_version': version
        })

        return {
            '_type': doc_type,
            '_id': document_id,
            'created': True,
            '_version': version,
            '_index': index
        }

    def get(self, index, id, doc_type='_all', params=None):
        result = None
        if index in self.__data_dict:
            for document in self.__data_dict[index]:
                if document.get('_id') == id:
                    if doc_type == '_all':
                        result = document
                        break
                    else:
                        if document.get('_type') == doc_type:
                            result = document
                            break

        if result:
            result['found'] = True
        else:
            error_data = {
                '_index': index,
                '_type': doc_type,
                '_id': id,
                'found': False
            }
            raise NotFoundError(404, json.dumps(error_data))

        return result

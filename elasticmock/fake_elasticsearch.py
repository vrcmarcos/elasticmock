# -*- coding: utf-8 -*-

import json

from elasticsearch import Elasticsearch, Transport
from elasticsearch.exceptions import NotFoundError

from elasticmock.utilities import get_random_id


class FakeElasticsearch(Elasticsearch):
    __documents_dict = None

    def __init__(self, hosts=None, transport_class=Transport, **kwargs):
        self.__documents_dict = {}

    def index(self, index, doc_type, body, document_id=None, params=None):
        if index not in self.__documents_dict:
            self.__documents_dict[index] = list()

        if document_id is None:
            document_id = get_random_id()

        version = 1

        self.__documents_dict[index].append({
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
        if index in self.__documents_dict:
            for document in self.__documents_dict[index]:
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

    def exists(self, index, doc_type, id, params=None):
        result = False
        if index in self.__documents_dict:
            for document in self.__documents_dict[index]:
                if document.get('_id') == id and document.get('_type') == doc_type:
                    result = True
                    break
        return result

    def ping(self, params=None):
        return True

    def info(self, params=None):
        return {
            'status': 200,
            'cluster_name': 'elasticmock',
            'version':
                {
                    'lucene_version': '4.10.4',
                    'build_hash': '00f95f4ffca6de89d68b7ccaf80d148f1f70e4d4',
                    'number': '1.7.5',
                    'build_timestamp': '2016-02-02T09:55:30Z',
                    'build_snapshot': False
                },
            'name': 'Nightwatch',
            'tagline': 'You Know, for Search'
        }

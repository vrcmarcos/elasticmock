# -*- coding: utf-8 -*-

import json

from elasticsearch import Elasticsearch
from elasticsearch.client.utils import query_params
from elasticsearch.exceptions import NotFoundError

from elasticmock.utilities import get_random_id


class FakeElasticsearch(Elasticsearch):
    __documents_dict = None

    def __init__(self):
        self.__documents_dict = {}

    @query_params()
    def ping(self, params=None):
        return True

    @query_params()
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

    @query_params('consistency', 'op_type', 'parent', 'refresh', 'replication',
                  'routing', 'timeout', 'timestamp', 'ttl', 'version', 'version_type')
    def index(self, index, doc_type, body, id=None, params=None):
        if index not in self.__documents_dict:
            self.__documents_dict[index] = list()

        if id is None:
            id = get_random_id()

        version = 1

        self.__documents_dict[index].append({
            '_type': doc_type,
            '_id': id,
            '_source': body,
            '_index': index,
            '_version': version
        })

        return {
            '_type': doc_type,
            '_id': id,
            'created': True,
            '_version': version,
            '_index': index
        }

    @query_params('parent', 'preference', 'realtime', 'refresh', 'routing')
    def exists(self, index, doc_type, id, params=None):
        result = False
        if index in self.__documents_dict:
            for document in self.__documents_dict[index]:
                if document.get('_id') == id and document.get('_type') == doc_type:
                    result = True
                    break
        return result

    @query_params('_source', '_source_exclude', '_source_include', 'fields',
                  'parent', 'preference', 'realtime', 'refresh', 'routing', 'version',
                  'version_type')
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

    @query_params('_source', '_source_exclude', '_source_include', 'parent',
                  'preference', 'realtime', 'refresh', 'routing', 'version',
                  'version_type')
    def get_source(self, index, doc_type, id, params=None):
        document = self.get(index=index, doc_type=doc_type, id=id, params=params)
        return document.get('_source')

    @query_params('_source', '_source_exclude', '_source_include',
                  'allow_no_indices', 'analyze_wildcard', 'analyzer', 'default_operator',
                  'df', 'expand_wildcards', 'explain', 'fielddata_fields', 'fields',
                  'from_', 'ignore_unavailable', 'lenient', 'lowercase_expanded_terms',
                  'preference', 'q', 'request_cache', 'routing', 'scroll', 'search_type',
                  'size', 'sort', 'stats', 'suggest_field', 'suggest_mode',
                  'suggest_size', 'suggest_text', 'terminate_after', 'timeout',
                  'track_scores', 'version')
    def count(self, index=None, doc_type=None, body=None, params=None):
        if index is not None and index not in self.__documents_dict:
            raise NotFoundError(404, 'IndexMissingException[[{0}] missing]'.format(index))

        searchable_indexes = [index] if index is not None else self.__documents_dict.keys()

        i = 0
        for searchable_index in searchable_indexes:
            for document in self.__documents_dict[searchable_index]:
                if doc_type is not None and document.get('_type') != doc_type:
                    continue
                i += 1
        result = {
            'count': i,
            '_shards': {
                'successful': 1,
                'failed': 0,
                'total': 1
            }
        }

        return result

    @query_params('_source', '_source_exclude', '_source_include',
                  'allow_no_indices', 'analyze_wildcard', 'analyzer', 'default_operator',
                  'df', 'expand_wildcards', 'explain', 'fielddata_fields', 'fields',
                  'from_', 'ignore_unavailable', 'lenient', 'lowercase_expanded_terms',
                  'preference', 'q', 'request_cache', 'routing', 'scroll', 'search_type',
                  'size', 'sort', 'stats', 'suggest_field', 'suggest_mode',
                  'suggest_size', 'suggest_text', 'terminate_after', 'timeout',
                  'track_scores', 'version')
    def search(self, index=None, doc_type=None, body=None, params=None):
        if index is not None and index not in self.__documents_dict:
            raise NotFoundError(404, 'IndexMissingException[[{0}] missing]'.format(index))

        searchable_indexes = [index] if index is not None else self.__documents_dict.keys()

        matches = []
        for searchable_index in searchable_indexes:
            for document in self.__documents_dict[searchable_index]:
                if doc_type is not None and document.get('_type') != doc_type:
                    continue
                matches.append(document)
        result = {
            'hits': {
                'total': len(matches),
                'max_score': 1.0
            },
            '_shards': {
                'successful': 1,
                'failed': 0,
                'total': 1
            },
            'took': 1,
            'timed_out': False
        }

        if matches:
            hits = []
            for match in matches:
                match['_score'] = 1.0
                hits.append(match)
            result['hits']['hits'] = hits

        return result

    @query_params('consistency', 'parent', 'refresh', 'replication', 'routing',
                  'timeout', 'version', 'version_type')
    def delete(self, index, doc_type, id, params=None):

        found = False

        if index in self.__documents_dict:
            for document in self.__documents_dict[index]:
                if document.get('_type') == doc_type and document.get('_id') == id:
                    found = True
                    self.__documents_dict[index].remove(document)
                    break

        result_dict = {
            'found': found,
            '_index': index,
            '_type': doc_type,
            '_id': id,
            '_version': 1,
        }

        if found:
            return result_dict
        else:
            raise NotFoundError(404, json.dumps(result_dict))

    @query_params('allow_no_indices', 'expand_wildcards', 'ignore_unavailable',
                  'preference', 'routing')
    def suggest(self, body, index=None, params=None):
        if index is not None and index not in self.__documents_dict:
            raise NotFoundError(404, 'IndexMissingException[[{0}] missing]'.format(index))

        result_dict = {}
        for key, value in body.items():
            text = value.get('text')
            suggestion = int(text) + 1 if isinstance(text, int) else '{0}_suggestion'.format(text)
            result_dict[key] = [
                {
                    'text': text,
                    'length': 1,
                    'options': [
                        {
                            'text': suggestion,
                            'freq': 1,
                            'score': 1.0
                        }
                    ],
                    'offset': 0
                }
            ]
        return result_dict

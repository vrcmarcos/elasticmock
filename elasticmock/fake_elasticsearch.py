# -*- coding: utf-8 -*-

import json
import sys

from elasticsearch import Elasticsearch
from elasticsearch.client.utils import query_params
from elasticsearch.exceptions import NotFoundError

from elasticmock.behaviour.server_failure import server_failure
from elasticmock.utilities import extract_ignore_as_iterable, get_random_id, get_random_scroll_id
from elasticmock.utilities.decorator import for_all_methods
from elasticmock.fake_indices import FakeIndicesClient
from elasticmock.fake_cluster import FakeClusterClient

PY3 = sys.version_info[0] == 3
if PY3:
    unicode = str


class QueryType:

    BOOL = 'BOOL'
    FILTER = 'FILTER'
    MATCH = 'MATCH'
    MATCH_ALL = 'MATCH_ALL'
    TERM = 'TERM'
    TERMS = 'TERMS'
    MUST = 'MUST'

    @staticmethod
    def get_query_type(type_str):
        if type_str == 'bool':
            return QueryType.BOOL
        elif type_str == 'filter':
            return QueryType.FILTER
        elif type_str == 'match':
            return QueryType.MATCH
        elif type_str == 'match_all':
            return QueryType.MATCH_ALL        
        elif type_str == 'term':
            return QueryType.TERM
        elif type_str == 'terms':
            return QueryType.TERMS
        elif type_str == 'must':
            return QueryType.MUST
        else:
            raise NotImplementedError(f'type {type_str} is not implemented for QueryType')


class FakeQueryCondition:
    type = None
    condition = None

    def __init__(self, type, condition):
        self.type = type
        self.condition = condition

    def evaluate(self, document):
        return self._evaluate_for_query_type(document)

    def _evaluate_for_query_type(self, document):
        if self.type == QueryType.MATCH:
            return self._evaluate_for_match_query_type(document)
        elif self.type == QueryType.TERM:
            return self._evaluate_for_term_query_type(document)
        elif self.type == QueryType.TERMS:
            return self._evaluate_for_terms_query_type(document)
        elif self.type == QueryType.BOOL:
            return self._evaluate_for_compound_query_type(document)
        elif self.type == QueryType.FILTER:
            return self._evaluate_for_compound_query_type(document)
        else:
            raise NotImplementedError('Fake query evaluation not implemented for query type: %s' % self.type)

    def _evaluate_for_match_query_type(self, document):
        return self._evaluate_for_field(document, True)

    def _evaluate_for_term_query_type(self, document):
        return self._evaluate_for_field(document, False)

    def _evaluate_for_terms_query_type(self, document):
        for field in self.condition:
            for term in self.condition[field]:
                if FakeQueryCondition(QueryType.TERM, {field: term}).evaluate(document):
                    return True
        return False

    def _evaluate_for_field(self, document, ignore_case):
        doc_source = document['_source']
        return_val = False
        for field, value in self.condition.items():
            return_val = self._compare_value_for_field(
                doc_source,
                field,
                value,
                ignore_case
            )
            if return_val:
                break
        return return_val

    def _evaluate_for_compound_query_type(self, document):
        return_val = False
        if isinstance(self.condition, dict):
            for query_type, sub_query in self.condition.items():
                return_val = FakeQueryCondition(
                    QueryType.get_query_type(query_type),
                    sub_query
                ).evaluate(document)
                if not return_val:
                    return False
        elif isinstance(self.condition, list):
            for sub_condition in self.condition:
                for sub_condition_key in sub_condition:
                    return_val = FakeQueryCondition(
                        QueryType.get_query_type(sub_condition_key),
                        sub_condition[sub_condition_key]
                    ).evaluate(document)
                    if not return_val:
                        return False

        return return_val

    def _compare_value_for_field(self, doc_source, field, value, ignore_case):
        value = str(value).lower() if ignore_case and isinstance(value, str) \
            else value
        doc_val = None
        if hasattr(doc_source, field):
            doc_val = getattr(doc_source, field)
        elif field in doc_source:
            doc_val = doc_source[field]

        if isinstance(doc_val, list):
            for val in doc_val:
                val = val if isinstance(val, (int, float, complex)) \
                    else str(val)
                if ignore_case and isinstance(val, str):
                    val = val.lower()
                if isinstance(val, str) and value in val:
                    return True
                if value == val:
                    return True
        else:
            doc_val = doc_val if isinstance(doc_val, (int, float, complex)) \
                else str(doc_val)
            if ignore_case and isinstance(doc_val, str):
                doc_val = doc_val.lower()
            if isinstance(doc_val, str) and value in doc_val:
                return True
            if value == doc_val:
                return True

        return False


@for_all_methods([server_failure])
class FakeElasticsearch(Elasticsearch):
    __documents_dict = None

    def __init__(self, hosts=None, transport_class=None, **kwargs):
        self.__documents_dict = {}
        self.__scrolls = {}

    @property
    def indices(self):
        return FakeIndicesClient(self)

    @property
    def cluster(self):
        return FakeClusterClient(self)

    @query_params()
    def ping(self, params=None, headers=None):
        return True

    @query_params()
    def info(self, params=None, headers=None):
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

    @query_params('consistency',
                  'op_type',
                  'parent',
                  'refresh',
                  'replication',
                  'routing',
                  'timeout',
                  'timestamp',
                  'ttl',
                  'version',
                  'version_type')
    def index(self, index, body, doc_type='_doc', id=None, params=None, headers=None):
        if index not in self.__documents_dict:
            self.__documents_dict[index] = list()
        
        version = 1

        if id is None:
            id = get_random_id()
        elif self.exists(index, doc_type, id, params=params):
            doc = self.get(index, id, doc_type, params=params)
            version = doc['_version'] + 1
            self.delete(index, doc_type, id)

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

    @query_params('consistency', 'op_type', 'parent', 'refresh', 'replication',
                  'routing', 'timeout', 'timestamp', 'ttl', 'version', 'version_type')
    def bulk(self,  body, index=None, doc_type=None, params=None, headers=None):
        version = 1
        items = []

        for line in body.splitlines():
            if len(line.strip()) > 0:
                line = json.loads(line)

                if 'index' in line:
                    index = line['index']['_index']
                    doc_type = line['index']['_type']

                    if index not in self.__documents_dict:
                        self.__documents_dict[index] = list()
                else:
                    document_id = get_random_id()

                    self.__documents_dict[index].append({
                        '_type': doc_type,
                        '_id': document_id,
                        '_source': line,
                        '_index': index,
                        '_version': version
                    })

                    items.append({'index': {
                        '_type': doc_type,
                        '_id': document_id,
                        '_index': index,
                        '_version': version,
                        'result': 'created',
                        'status': 201
                    }})

        return {
            'errors': False,
            'items': items
        }

    @query_params('parent', 'preference', 'realtime', 'refresh', 'routing')
    def exists(self, index, doc_type, id, params=None, headers=None):
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
    def get(self, index, id, doc_type='_all', params=None, headers=None):
        ignore = extract_ignore_as_iterable(params)
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
            return result
        elif params and 404 in ignore:
            return {'found': False}
        else:
            error_data = {
                '_index': index,
                '_type': doc_type,
                '_id': id,
                'found': False
            }
            raise NotFoundError(404, json.dumps(error_data))


    @query_params('_source', '_source_exclude', '_source_include', 'parent',
                  'preference', 'realtime', 'refresh', 'routing', 'version',
                  'version_type')
    def get_source(self, index, doc_type, id, params=None, headers=None):
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
    def count(self, index=None, doc_type=None, body=None, params=None, headers=None):
        searchable_indexes = self._normalize_index_to_list(index)

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

    def _get_fake_query_condition(self, query_type_str, condition):
        return FakeQueryCondition(QueryType.get_query_type(query_type_str), condition)

    @query_params('_source', '_source_exclude', '_source_include',
                  'allow_no_indices', 'analyze_wildcard', 'analyzer', 'default_operator',
                  'df', 'expand_wildcards', 'explain', 'fielddata_fields', 'fields',
                  'from_', 'ignore_unavailable', 'lenient', 'lowercase_expanded_terms',
                  'preference', 'q', 'request_cache', 'routing', 'scroll', 'search_type',
                  'size', 'sort', 'stats', 'suggest_field', 'suggest_mode',
                  'suggest_size', 'suggest_text', 'terminate_after', 'timeout',
                  'track_scores', 'version')
    def search(self, index=None, doc_type=None, body=None, params=None, headers=None):
        searchable_indexes = self._normalize_index_to_list(index)

        matches = []
        conditions = []

        if body and 'query' in body:
            query = body['query']
            for query_type_str, condition in query.items():
                conditions.append(self._get_fake_query_condition(query_type_str, condition))
        for searchable_index in searchable_indexes:
            for document in self.__documents_dict[searchable_index]:
                if doc_type:
                    if isinstance(doc_type, list) and document.get('_type') not in doc_type:
                        continue
                    if isinstance(doc_type, str) and document.get('_type') != doc_type:
                        continue
                if conditions:
                    for condition in conditions:
                        if condition.evaluate(document):
                            matches.append(document)
                            break
                else:
                    matches.append(document)

        result = {
            'hits': {
                'total': len(matches),
                'max_score': 1.0
            },
            '_shards': {
                # Simulate indexes with 1 shard each
                'successful': len(searchable_indexes),
                'failed': 0,
                'total': len(searchable_indexes)
            },
            'took': 1,
            'timed_out': False
        }

        hits = []
        for match in matches:
            match['_score'] = 1.0
            hits.append(match)

        # build aggregations
        if body is not None and 'aggs' in body:
            aggregations = {}

            for aggregation, definition in body['aggs'].items():
                aggregations[aggregation] = {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    "buckets": []
                }

            if aggregations:
                result['aggregations'] = aggregations

        if 'scroll' in params:
            result['_scroll_id'] = str(get_random_scroll_id())
            params['size'] = int(params.get('size') if 'size' in params else 10)
            params['from'] = int(params.get('from') + params.get('size') if 'from' in params else 0)
            self.__scrolls[result.get('_scroll_id')] = {
                'index': index,
                'doc_type': doc_type,
                'body': body,
                'params': params
            }
            hits = hits[params.get('from'):params.get('from') + params.get('size')]
        
        result['hits']['hits'] = hits
        
        return result

    @query_params('scroll')
    def scroll(self, scroll_id, params=None, headers=None):
        scroll = self.__scrolls.pop(scroll_id)
        result = self.search(
            index=scroll.get('index'),
            doc_type=scroll.get('doc_type'),
            body=scroll.get('body'),
            params=scroll.get('params')
        )
        return result
    
    @query_params('consistency', 'parent', 'refresh', 'replication', 'routing',
                  'timeout', 'version', 'version_type')
    def delete(self, index, doc_type, id, params=None, headers=None):

        found = False
        ignore = extract_ignore_as_iterable(params)

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
        elif params and 404 in ignore:
            return {'found': False}
        else:
            raise NotFoundError(404, json.dumps(result_dict))

    @query_params('allow_no_indices', 'expand_wildcards', 'ignore_unavailable',
                  'preference', 'routing')
    def suggest(self, body, index=None, params=None, headers=None):
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

    def _normalize_index_to_list(self, index):
        # Ensure to have a list of index
        if index is None:
            searchable_indexes = self.__documents_dict.keys()
        elif isinstance(index, str) or isinstance(index, unicode):
            searchable_indexes = [index]
        elif isinstance(index, list):
            searchable_indexes = index
        else:
            # Is it the correct exception to use ?
            raise ValueError("Invalid param 'index'")

        # Check index(es) exists
        for searchable_index in searchable_indexes:
            if searchable_index not in self.__documents_dict:
                raise NotFoundError(404, 'IndexMissingException[[{0}] missing]'.format(searchable_index))

        return searchable_indexes

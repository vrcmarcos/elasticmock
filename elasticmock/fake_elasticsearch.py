# -*- coding: utf-8 -*-
import datetime
import json
import sys
from collections import defaultdict

import dateutil.parser
from elasticsearch import Elasticsearch
from elasticsearch.client.utils import query_params
from elasticsearch.client import _normalize_hosts
from elasticsearch.transport import Transport
from elasticsearch.exceptions import NotFoundError, RequestError

from elasticmock.behaviour.server_failure import server_failure
from elasticmock.fake_cluster import FakeClusterClient
from elasticmock.fake_indices import FakeIndicesClient
from elasticmock.utilities import (extract_ignore_as_iterable, get_random_id,
    get_random_scroll_id)
from elasticmock.utilities.decorator import for_all_methods

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
    RANGE = 'RANGE'
    SHOULD = 'SHOULD'
    MINIMUM_SHOULD_MATCH = 'MINIMUM_SHOULD_MATCH'
    MULTI_MATCH = 'MULTI_MATCH'
    MUST_NOT = 'MUST_NOT'

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
        elif type_str == 'range':
            return QueryType.RANGE
        elif type_str == 'should':
            return QueryType.SHOULD
        elif type_str == 'minimum_should_match':
            return QueryType.MINIMUM_SHOULD_MATCH
        elif type_str == 'multi_match':
            return QueryType.MULTI_MATCH
        elif type_str == 'must_not':
            return QueryType.MUST_NOT
        else:
            raise NotImplementedError(f'type {type_str} is not implemented for QueryType')


class MetricType:
    CARDINALITY = "CARDINALITY"

    @staticmethod
    def get_metric_type(type_str):
        if type_str == "cardinality":
            return MetricType.CARDINALITY
        else:
            raise NotImplementedError(f'type {type_str} is not implemented for MetricType')


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
        elif self.type == QueryType.MATCH_ALL:
            return True
        elif self.type == QueryType.TERM:
            return self._evaluate_for_term_query_type(document)
        elif self.type == QueryType.TERMS:
            return self._evaluate_for_terms_query_type(document)
        elif self.type == QueryType.RANGE:
            return self._evaluate_for_range_query_type(document)
        elif self.type == QueryType.BOOL:
            return self._evaluate_for_compound_query_type(document)
        elif self.type == QueryType.FILTER:
            return self._evaluate_for_compound_query_type(document)
        elif self.type == QueryType.MUST:
            return self._evaluate_for_compound_query_type(document)
        elif self.type == QueryType.SHOULD:
            return self._evaluate_for_should_query_type(document)
        elif self.type == QueryType.MULTI_MATCH:
            return self._evaluate_for_multi_match_query_type(document)
        elif self.type == QueryType.MUST_NOT:
            return self._evaluate_for_must_not_query_type(document)
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

    def _evaluate_for_fields(self, document):
        doc_source = document['_source']
        return_val = False
        value = self.condition.get('query')
        if not value:
            return return_val
        fields = self.condition.get('fields', [])
        for field in fields:
            return_val = self._compare_value_for_field(
                doc_source,
                field,
                value,
                True
            )
            if return_val:
                break

        return return_val

    def _evaluate_for_range_query_type(self, document):
        for field, comparisons in self.condition.items():
            doc_val = document['_source']
            for k in field.split("."):
                if hasattr(doc_val, k):
                    doc_val = getattr(doc_val, k)
                elif k in doc_val:
                    doc_val = doc_val[k]
                else:
                    return False

            if isinstance(doc_val, list):
                return False

            for sign, value in comparisons.items():
                if isinstance(doc_val, datetime.datetime):
                    value = dateutil.parser.isoparse(value)
                if sign == 'gte':
                    if doc_val < value:
                        return False
                elif sign == 'gt':
                    if doc_val <= value:
                        return False
                elif sign == 'lte':
                    if doc_val > value:
                        return False
                elif sign == 'lt':
                    if doc_val >= value:
                        return False
                else:
                    raise ValueError(f"Invalid comparison type {sign}")
            return True

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

    def _evaluate_for_must_not_query_type(self, document):
        if isinstance(self.condition, dict):
            for query_type, sub_query in self.condition.items():
                return_val = FakeQueryCondition(
                    QueryType.get_query_type(query_type),
                    sub_query
                ).evaluate(document)
                if return_val:
                    return False
        elif isinstance(self.condition, list):
            for sub_condition in self.condition:
                for sub_condition_key in sub_condition:
                    return_val = FakeQueryCondition(
                        QueryType.get_query_type(sub_condition_key),
                        sub_condition[sub_condition_key]
                    ).evaluate(document)
                    if return_val:
                        return False
        return True

    def _evaluate_for_should_query_type(self, document):
        return_val = False
        for sub_condition in self.condition:
            for sub_condition_key in sub_condition:
                return_val = FakeQueryCondition(
                    QueryType.get_query_type(sub_condition_key),
                    sub_condition[sub_condition_key]
                ).evaluate(document)
                if return_val:
                    return True
        return return_val

    def _evaluate_for_multi_match_query_type(self, document):
        return self._evaluate_for_fields(document)

    def _compare_value_for_field(self, doc_source, field, value, ignore_case):
        if ignore_case and isinstance(value, str):
            value = value.lower()

        doc_val = doc_source
        # Remove boosting
        field, *_ = field.split("*")
        for k in field.split("."):
            if hasattr(doc_val, k):
                doc_val = getattr(doc_val, k)
                break
            elif k in doc_val:
                doc_val = doc_val[k]
                break
            else:
                return False

        if not isinstance(doc_val, list):
            doc_val = [doc_val]

        for val in doc_val:
            if not isinstance(val, (int, float, complex)) or val is None:
                val = str(val)
                if ignore_case:
                    val = val.lower()

            if value == val:
                return True
            if isinstance(val, str) and str(value) in val:
                return True

        return False


@for_all_methods([server_failure])
class FakeElasticsearch(Elasticsearch):
    __documents_dict = None

    def __init__(self, hosts=None, transport_class=None, **kwargs):
        self.__documents_dict = {}
        self.__scrolls = {}
        self.transport = Transport(_normalize_hosts(hosts), **kwargs)

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
        elif self.exists(index, id, doc_type=doc_type, params=params):
            doc = self.get(index, id, doc_type=doc_type, params=params)
            version = doc['_version'] + 1
            self.delete(index, id, doc_type=doc_type)

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
    def bulk(self, body, index=None, doc_type=None, params=None, headers=None):
        items = []
        errors = False

        for raw_line in body.splitlines():
            if len(raw_line.strip()) > 0:
                line = json.loads(raw_line)

                if any(action in line for action in ['index', 'create', 'update', 'delete']):
                    action = next(iter(line.keys()))

                    version = 1
                    index = line[action].get('_index') or index
                    doc_type = line[action].get('_type', "_doc")  # _type is deprecated in 7.x

                    if action in ['delete', 'update'] and not line[action].get("_id"):
                        raise RequestError(400, 'action_request_validation_exception', 'missing id')

                    document_id = line[action].get('_id', get_random_id())

                    if action == 'delete':
                        status, result, error = self._validate_action(
                            action, index, document_id, doc_type, params=params
                        )
                        item = {action: {
                            '_type': doc_type,
                            '_id': document_id,
                            '_index': index,
                            '_version': version,
                            'status': status,
                        }}
                        if error:
                            errors = True
                            item[action]["error"] = result
                        else:
                            self.delete(index, document_id, doc_type=doc_type, params=params)
                            item[action]["result"] = result
                        items.append(item)

                    if index not in self.__documents_dict:
                        self.__documents_dict[index] = list()
                else:
                    if 'doc' in line and action == 'update':
                        source = line['doc']
                    else:
                        source = line
                    status, result, error = self._validate_action(
                        action, index, document_id, doc_type, params=params
                    )
                    item = {
                        action: {
                            '_type': doc_type,
                            '_id': document_id,
                            '_index': index,
                            '_version': version,
                            'status': status,
                        }
                    }
                    if not error:
                        item[action]["result"] = result
                        if self.exists(index, document_id, doc_type=doc_type, params=params):
                            doc = self.get(index, document_id, doc_type=doc_type, params=params)
                            version = doc['_version'] + 1
                            self.delete(index, document_id, doc_type=doc_type, params=params)

                        self.__documents_dict[index].append({
                            '_type': doc_type,
                            '_id': document_id,
                            '_source': source,
                            '_index': index,
                            '_version': version
                        })
                    else:
                        errors = True
                        item[action]["error"] = result
                    items.append(item)
        return {
            'errors': errors,
            'items': items
        }

    def _validate_action(self, action, index, document_id, doc_type, params=None):
        if action in ['index', 'update'] and self.exists(index, id=document_id, doc_type=doc_type, params=params):
            return 200, 'updated', False
        if action == 'create' and self.exists(index, id=document_id, doc_type=doc_type, params=params):
            return 409, 'version_conflict_engine_exception', True
        elif action in ['index', 'create'] and not self.exists(index, id=document_id, doc_type=doc_type, params=params):
            return 201, 'created', False
        elif action == "delete" and self.exists(index, id=document_id, doc_type=doc_type, params=params):
            return 200, 'deleted', False
        elif action == 'update' and not self.exists(index, id=document_id, doc_type=doc_type, params=params):
            return 404, 'document_missing_exception', True
        elif action == 'delete' and not self.exists(index, id=document_id, doc_type=doc_type, params=params):
            return 404, 'not_found', True
        else:
            raise NotImplementedError(f"{action} behaviour hasn't been implemented")

    @query_params('parent', 'preference', 'realtime', 'refresh', 'routing')
    def exists(self, index, id, doc_type=None, params=None, headers=None):
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

    @query_params('_source', '_source_exclude', '_source_include',
                  'preference', 'realtime', 'refresh', 'routing',
                  'stored_fields')
    def mget(self, body, index, doc_type='_all', params=None, headers=None):
        ids = body.get('ids')
        results = []
        for id in ids:
            try:
                results.append(self.get(index, id, doc_type=doc_type,
                    params=params, headers=headers))
            except:
                pass
        if not results:
            raise RequestError(
                400,
                'action_request_validation_exception',
                'Validation Failed: 1: no documents to get;'
            )
        return {'docs': results}

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
                if doc_type and document.get('_type') != doc_type:
                    continue
                i += 1
        result = {
            'count': i,
            '_shards': {
                'successful': 1,
                'skipped': 0,
                'failed': 0,
                'total': 1
            }
        }

        return result

    def _get_fake_query_condition(self, query_type_str, condition):
        return FakeQueryCondition(QueryType.get_query_type(query_type_str), condition)

    @query_params(
        "ccs_minimize_roundtrips",
        "max_concurrent_searches",
        "max_concurrent_shard_requests",
        "pre_filter_shard_size",
        "rest_total_hits_as_int",
        "search_type",
        "typed_keys",
    )
    def msearch(self, body, index=None, doc_type=None, params=None, headers=None):
        def grouped(iterable):
            if len(iterable) % 2 != 0:
                raise Exception('Malformed body')
            iterator = iter(iterable)
            while True:
                try:
                    yield (next(iterator)['index'], next(iterator))
                except StopIteration:
                    break

        responses = []
        took = 0
        for ind, query in grouped(body):
            response = self.search(index=ind, body=query)
            took += response['took']
            responses.append(response)
        result = {
            'took': took,
            'responses': responses
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

        for match in matches:
            self._find_and_convert_data_types(match['_source'])

        result = {
            'hits': {
                'total': {'value': len(matches), 'relation': 'eq'},
                'max_score': 1.0
            },
            '_shards': {
                # Simulate indexes with 1 shard each
                'successful': len(searchable_indexes),
                'skipped': 0,
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
                    "buckets": self.make_aggregation_buckets(definition, matches)
                }

            if aggregations:
                result['aggregations'] = aggregations

        if 'scroll' in params:
            result['_scroll_id'] = str(get_random_scroll_id())
            params['size'] = int(params.get('size', 10))
            params['from'] = int(params.get('from') + params.get('size') if 'from' in params else 0)
            self.__scrolls[result.get('_scroll_id')] = {
                'index': index,
                'doc_type': doc_type,
                'body': body,
                'params': params
            }
            hits = hits[params.get('from'):params.get('from') + params.get('size')]
        elif 'size' in params:
            hits = hits[:int(params['size'])]

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
    def delete(self, index, id, doc_type=None, params=None, headers=None):

        found = False
        ignore = extract_ignore_as_iterable(params)

        if index in self.__documents_dict:
            for document in self.__documents_dict[index]:
                if document.get('_id') == id:
                    found = True
                    if doc_type and document.get('_type') != doc_type:
                        found = False
                    if found:
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

    @classmethod
    def _find_and_convert_data_types(cls, document):
        for key, value in document.items():
            if isinstance(value, dict):
                cls._find_and_convert_data_types(value)
            elif isinstance(value, datetime.datetime):
                document[key] = value.isoformat()

    def make_aggregation_buckets(self, aggregation, documents):
        if 'composite' in aggregation:
            return self.make_composite_aggregation_buckets(aggregation, documents)
        return []

    def make_composite_aggregation_buckets(self, aggregation, documents):

        def make_key(doc_source, agg_source):
            attr = list(agg_source.values())[0]["terms"]["field"]
            return doc_source[attr]

        def make_bucket(bucket_key, bucket):
            out = {
                "key": {k: v for k, v in zip(bucket_key_fields, bucket_key)},
                "doc_count": len(bucket),
            }

            for metric_key, metric_definition in aggregation["aggs"].items():
                metric_type_str = list(metric_definition)[0]
                metric_type = MetricType.get_metric_type(metric_type_str)
                attr = metric_definition[metric_type_str]["field"]
                data = [doc[attr] for doc in bucket]

                if metric_type == MetricType.CARDINALITY:
                    value = len(set(data))
                else:
                    raise NotImplementedError(f"Metric type '{metric_type}' not implemented")

                out[metric_key] = {"value": value}
            return out

        agg_sources = aggregation["composite"]["sources"]
        buckets = defaultdict(list)
        bucket_key_fields = [list(src)[0] for src in agg_sources]
        for document in documents:
            doc_src = document["_source"]
            key = tuple(make_key(doc_src, agg_src) for agg_src in aggregation["composite"]["sources"])
            buckets[key].append(doc_src)

        buckets = sorted(((k, v) for k, v in buckets.items()), key=lambda x: x[0])
        buckets = [make_bucket(bucket_key, bucket) for bucket_key, bucket in buckets]
        return buckets

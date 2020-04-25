# -*- coding: utf-8 -*-

from elasticsearch.client.indices import IndicesClient
from elasticsearch.client.utils import query_params


class FakeIndicesClient(IndicesClient):

    @query_params('master_timeout', 'timeout')
    def create(self, index, body=None, params=None, headers=None):
        documents_dict = self.__get_documents_dict()
        if index not in documents_dict:
            documents_dict[index] = []

    @query_params('allow_no_indices', 'expand_wildcards', 'ignore_unavailable',
                  'local')
    def exists(self, index, params=None, headers=None):
        return index in self.__get_documents_dict()

    @query_params('allow_no_indices', 'expand_wildcards', 'force',
                  'ignore_unavailable', 'operation_threading')
    def refresh(self, index=None, params=None, headers=None):
        pass

    @query_params('master_timeout', 'timeout')
    def delete(self, index, params=None, headers=None):
        documents_dict = self.__get_documents_dict()
        if index in documents_dict:
            del documents_dict[index]

    def __get_documents_dict(self):
        return self.client._FakeElasticsearch__documents_dict

# -*- coding: utf-8 -*-

from elasticsearch.exceptions import NotFoundError

from tests import TestElasticmock, INDEX_NAME, DOC_TYPE, BODY


class TestDelete(TestElasticmock):

    def test_should_raise_exception_when_delete_nonindexed_document(self):
        with self.assertRaises(NotFoundError):
            self.es.delete(index=INDEX_NAME, doc_type=DOC_TYPE, id=1)

    def test_should_not_raise_exception_when_delete_nonindexed_document_if_ignored(self):
        target_doc = self.es.delete(index=INDEX_NAME, doc_type=DOC_TYPE, id=1, ignore=404)
        self.assertFalse(target_doc.get('found'))

    def test_should_not_raise_exception_when_delete_nonindexed_document_if_ignored_list(self):
        target_doc = self.es.delete(index=INDEX_NAME, doc_type=DOC_TYPE, id=1, ignore=(401, 404))
        self.assertFalse(target_doc.get('found'))

    def test_should_delete_indexed_document(self):
        doc_indexed = self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=BODY)
        search = self.es.search(index=INDEX_NAME)
        self.assertEqual(1, search.get('hits').get('total').get('value'))

        doc_id = doc_indexed.get('_id')
        doc_deleted = self.es.delete(index=INDEX_NAME, doc_type=DOC_TYPE, id=doc_id)
        search = self.es.search(index=INDEX_NAME)
        self.assertEqual(0, search.get('hits').get('total').get('value'))

        expected_doc_deleted = {
            'found': True,
            '_index': INDEX_NAME,
            '_type': DOC_TYPE,
            '_id': doc_id,
            '_version': 1,
        }

        self.assertDictEqual(expected_doc_deleted, doc_deleted)

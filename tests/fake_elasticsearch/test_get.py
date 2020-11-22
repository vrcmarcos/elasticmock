# -*- coding: utf-8 -*-

from elasticsearch.exceptions import NotFoundError

from tests import TestElasticmock, INDEX_NAME, DOC_TYPE, BODY


class TestGet(TestElasticmock):

    def test_should_raise_notfounderror_when_nonindexed_id_is_used(self):
        with self.assertRaises(NotFoundError):
            self.es.get(index=INDEX_NAME, id='1')

    def test_should_not_raise_notfounderror_when_nonindexed_id_is_used_and_ignored(self):
        target_doc = self.es.get(index=INDEX_NAME, id='1', ignore=404)
        self.assertFalse(target_doc.get('found'))

    def test_should_not_raise_notfounderror_when_nonindexed_id_is_used_and_ignored_list(self):
        target_doc = self.es.get(index=INDEX_NAME, id='1', ignore=(401, 404))
        self.assertFalse(target_doc.get('found'))

    def test_should_get_document_with_id(self):
        data = self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=BODY)

        document_id = data.get('_id')
        target_doc = self.es.get(index=INDEX_NAME, id=document_id)

        expected = {
            '_type': DOC_TYPE,
            '_source': BODY,
            '_index': INDEX_NAME,
            '_version': 1,
            'found': True,
            '_id': document_id
        }

        self.assertDictEqual(expected, target_doc)

    def test_should_get_document_with_id_and_doc_type(self):
        data = self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=BODY)

        document_id = data.get('_id')
        target_doc = self.es.get(index=INDEX_NAME, id=document_id, doc_type=DOC_TYPE)

        expected = {
            '_type': DOC_TYPE,
            '_source': BODY,
            '_index': INDEX_NAME,
            '_version': 1,
            'found': True,
            '_id': document_id
        }

        self.assertDictEqual(expected, target_doc)

    def test_should_get_only_document_source_with_id(self):
        data = self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=BODY)

        document_id = data.get('_id')
        target_doc_source = self.es.get_source(index=INDEX_NAME, doc_type=DOC_TYPE, id=document_id)

        self.assertEqual(target_doc_source, BODY)

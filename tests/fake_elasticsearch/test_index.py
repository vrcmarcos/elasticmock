# -*- coding: utf-8 -*-

from tests import TestElasticmock, INDEX_NAME, DOC_TYPE, BODY

UPDATED_BODY = {
    'author': 'vrcmarcos',
    'text': 'Updated Text'
}


class TestIndex(TestElasticmock):

    def test_should_index_document(self):
        data = self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=BODY)

        self.assertEqual(DOC_TYPE, data.get('_type'))
        self.assertTrue(data.get('created'))
        self.assertEqual(1, data.get('_version'))
        self.assertEqual(INDEX_NAME, data.get('_index'))

    def test_should_index_document_without_doc_type(self):
        data = self.es.index(index=INDEX_NAME, body=BODY)

        self.assertEqual('_doc', data.get('_type'))
        self.assertTrue(data.get('created'))
        self.assertEqual(1, data.get('_version'))
        self.assertEqual(INDEX_NAME, data.get('_index'))

    def test_doc_type_can_be_list(self):
        doc_types = ['1_idx', '2_idx', '3_idx']
        count_per_doc_type = 3

        for doc_type in doc_types:
            for _ in range(count_per_doc_type):
                self.es.index(index=INDEX_NAME, doc_type=doc_type, body={})

        result = self.es.search(doc_type=[doc_types[0]])
        self.assertEqual(count_per_doc_type, result.get('hits').get('total'))

        result = self.es.search(doc_type=doc_types[:2])
        self.assertEqual(count_per_doc_type * 2, result.get('hits').get('total'))

    def test_update_existing_doc(self):
        data = self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=BODY)
        document_id = data.get('_id')
        self.es.index(index=INDEX_NAME, id=document_id, doc_type=DOC_TYPE, body=UPDATED_BODY)
        target_doc = self.es.get(index=INDEX_NAME, id=document_id)

        expected = {
            '_type': DOC_TYPE,
            '_source': UPDATED_BODY,
            '_index': INDEX_NAME,
            '_version': 2,
            'found': True,
            '_id': document_id
        }

        self.assertDictEqual(expected, target_doc)

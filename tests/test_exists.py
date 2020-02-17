# -*- coding: utf-8 -*-

from tests import TestElasticmock, INDEX_NAME, DOC_TYPE, BODY


class TestExists(TestElasticmock):

    def test_should_return_exists_false_if_nonindexed_id_is_used(self):
        self.assertFalse(self.es.exists(index=INDEX_NAME, doc_type=DOC_TYPE, id=1))

    def test_should_return_exists_true_if_indexed_id_is_used(self):
        data = self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=BODY)
        document_id = data.get('_id')
        self.assertTrue(self.es.exists(index=INDEX_NAME, doc_type=DOC_TYPE, id=document_id))

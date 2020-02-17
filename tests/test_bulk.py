# -*- coding: utf-8 -*-

import json

from tests import TestElasticmock, INDEX_NAME, DOC_TYPE, BODY


class TestBulk(TestElasticmock):

    def test_should_bulk_index_documents(self):
        action = {'index': {'_index': INDEX_NAME, '_type': DOC_TYPE}}
        action_json = json.dumps(action)
        body_json = json.dumps(BODY, default=str)
        num_of_documents = 10

        lines = []
        for count in range(0, num_of_documents):
            lines.append(action_json)
            lines.append(body_json)
        body = '\n'.join(lines)

        data = self.es.bulk(body=body)
        items = data.get('items')

        self.assertFalse(data.get('errors'))
        self.assertEqual(num_of_documents, len(items))

        for item in items:
            index = item.get('index')

            self.assertEqual(DOC_TYPE, index.get('_type'))
            self.assertEqual(INDEX_NAME, index.get('_index'))
            self.assertEqual('created', index.get('result'))
            self.assertEqual(201, index.get('status'))

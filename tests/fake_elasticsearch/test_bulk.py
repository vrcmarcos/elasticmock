# -*- coding: utf-8 -*-

import json

from tests import TestElasticmock, INDEX_NAME, DOC_TYPE, BODY, DOC_ID


class TestBulk(TestElasticmock):

    def test_should_bulk_index_documents_index_creates(self):
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

    def test_should_bulk_index_documents_create_creates(self):
        create_action = {'create': {'_index': INDEX_NAME, '_type': DOC_TYPE}}
        create_with_id = {'create': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': DOC_ID}}
        actions = [
            json.dumps(create_action),
            json.dumps(BODY, default=str),
            json.dumps(create_action),
            json.dumps(BODY, default=str),
            json.dumps(create_with_id),
            json.dumps(BODY, default=str),
            # Will fail on created document with the same ID
            json.dumps(create_with_id),
            json.dumps(BODY, default=str),
        ]
        body = '\n'.join(actions)

        data = self.es.bulk(body=body)

        items = data.get('items')

        self.assertTrue(data.get('errors'))
        self.assertEqual(4, len(items))

        last_item = items.pop()
        self.assertEqual(last_item['create']['error'], 'version_conflict_engine_exception')
        self.assertEqual(last_item['create']['status'], 409)
        for item in items:
            index = item.get('create')
            self.assertEqual(DOC_TYPE, index.get('_type'))
            self.assertEqual(INDEX_NAME, index.get('_index'))
            self.assertEqual('created', index.get('result'))
            self.assertEqual(201, index.get('status'))

    def test_should_bulk_index_documents_index_updates(self):
        action = {'index': {'_index': INDEX_NAME, '_id': DOC_ID, '_type': DOC_TYPE}}
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

        first_item = items.pop(0)
        self.assertEqual(first_item["index"]["status"], 201)
        self.assertEqual(first_item["index"]["result"], "created")

        for item in items:
            index = item.get('index')
            self.assertEqual(DOC_TYPE, index.get('_type'))
            self.assertEqual(INDEX_NAME, index.get('_index'))
            self.assertEqual('updated', index.get('result'))
            self.assertEqual(200, index.get('status'))

    def test_should_bulk_index_documents_update_updates(self):
        action = {'update': {'_index': INDEX_NAME, '_id': DOC_ID, '_type': DOC_TYPE}}
        action_json = json.dumps(action)
        create_action_json = json.dumps(
            {'create': {'_index': INDEX_NAME, '_id': DOC_ID, '_type': DOC_TYPE}}
        )
        body_json = json.dumps({'doc': BODY}, default=str)
        num_of_documents = 4

        lines = [create_action_json, json.dumps(BODY, default=str)]
        for count in range(0, num_of_documents):
            lines.append(action_json)
            lines.append(body_json)
        body = '\n'.join(lines)

        data = self.es.bulk(body=body)
        items = data.get('items')

        self.assertFalse(data.get('errors'))
        self.assertEqual(num_of_documents + 1, len(items))

        first_item = items.pop(0)
        self.assertEqual(first_item["create"]["status"], 201)
        self.assertEqual(first_item["create"]["result"], "created")

        for item in items:
            index = item.get('update')
            self.assertEqual(DOC_TYPE, index.get('_type'))
            self.assertEqual(INDEX_NAME, index.get('_index'))
            self.assertEqual('updated', index.get('result'))
            self.assertEqual(200, index.get('status'))

    def test_should_bulk_index_documents_delete_deletes(self):
        delete_action = {'delete': {'_index': INDEX_NAME, '_id': DOC_ID, '_type': DOC_TYPE}}
        delete_action_json = json.dumps(delete_action)
        create_action_json = json.dumps(
            {'create': {'_index': INDEX_NAME, '_id': DOC_ID, '_type': DOC_TYPE}}
        )

        lines = [
            create_action_json,
            json.dumps(BODY, default=str),
            delete_action_json,
        ]
        body = '\n'.join(lines)

        data = self.es.bulk(body=body)
        items = data.get('items')

        self.assertFalse(data.get('errors'))
        self.assertEqual(2, len(items))

        first_item = items.pop(0)
        self.assertEqual(first_item["create"]["status"], 201)
        self.assertEqual(first_item["create"]["result"], "created")
        self.assertEqual(first_item["create"]['_type'], DOC_TYPE)
        self.assertEqual(first_item["create"]['_id'], DOC_ID)

        second_item = items.pop(0)
        self.assertEqual(second_item["delete"]["status"], 200)
        self.assertEqual(second_item["delete"]["result"], "deleted")
        self.assertEqual(second_item["delete"]['_type'], DOC_TYPE)
        self.assertEqual(second_item["delete"]['_id'], DOC_ID)

    def test_should_bulk_index_documents_mixed_actions(self):
        doc_body = json.dumps(BODY, default=str)

        doc_id_1 = 1
        doc_id_2 = 2
        actions = [
            json.dumps({'create': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': doc_id_1}}),
            doc_body,  # 201
            json.dumps({'create': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': doc_id_1}}),
            doc_body,  # 409
            json.dumps({'index': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': doc_id_2}}),
            doc_body,  # 201
            json.dumps({'index': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': doc_id_2}}),
            doc_body,  # 200
            json.dumps({'update': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': doc_id_1}}),
            doc_body,  # 200
            json.dumps({'delete': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': doc_id_1}}),
            # 200
            json.dumps({'update': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': doc_id_1}}),
            doc_body,  # 404
            json.dumps({'delete': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': doc_id_1}}),
            # 404
        ]
        body = '\n'.join(actions)

        data = self.es.bulk(body=body)

        expected = [
            {'create': {'_type': DOC_TYPE, '_id': 1, '_index': INDEX_NAME,
                        '_version': 1, 'status': 201, 'result': 'created'}},
            {'create': {'_type': DOC_TYPE, '_id': 1, '_index': INDEX_NAME,
                        '_version': 1, 'status': 409,
                        'error': 'version_conflict_engine_exception'}},
            {'index': {'_type': DOC_TYPE, '_id': 2, '_index': INDEX_NAME,
                       '_version': 1, 'status': 201, 'result': 'created'}},
            {'index': {'_type': DOC_TYPE, '_id': 2, '_index': INDEX_NAME,
                       '_version': 1, 'status': 200, 'result': 'updated'}},
            {'update': {'_type': DOC_TYPE, '_id': 1, '_index': INDEX_NAME,
                        '_version': 1, 'status': 200, 'result': 'updated'}},
            {'delete': {'_type': DOC_TYPE, '_id': 1, '_index': INDEX_NAME,
                        '_version': 1, 'result': 'deleted', 'status': 200}},
            {'update': {'_type': DOC_TYPE, '_id': 1, '_index': INDEX_NAME,
                        '_version': 1, 'status': 404, 'error': 'document_missing_exception'}},
            {'delete': {'_type': DOC_TYPE, '_id': 1, '_index': INDEX_NAME,
                        '_version': 1, 'error': 'not_found', 'status': 404}},
            ]

        actual = data.get('items')

        self.assertTrue(data.get('errors'))
        self.assertEqual(actual, expected)

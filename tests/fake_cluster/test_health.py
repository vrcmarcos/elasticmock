# -*- coding: utf-8 -*-

from tests import TestElasticmock


class TestHealth(TestElasticmock):

    def test_should_return_health(self):
        health_status = self.es.cluster.health()

        expected_health_status = {
            'cluster_name': 'testcluster',
            'status': 'green',
            'timed_out': False,
            'number_of_nodes': 1,
            'number_of_data_nodes': 1,
            'active_primary_shards': 1,
            'active_shards': 1,
            'relocating_shards': 0,
            'initializing_shards': 0,
            'unassigned_shards': 1,
            'delayed_unassigned_shards': 0,
            'number_of_pending_tasks': 0,
            'number_of_in_flight_fetch': 0,
            'task_max_waiting_in_queue_millis': 0,
            'active_shards_percent_as_number': 50.0
        }

        self.assertDictEqual(expected_health_status, health_status)

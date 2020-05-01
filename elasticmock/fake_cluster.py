# -*- coding: utf-8 -*-

from elasticsearch.client.cluster import ClusterClient
from elasticsearch.client.utils import query_params


class FakeClusterClient(ClusterClient):

    @query_params('level', 'local', 'master_timeout', 'timeout',
                  'wait_for_active_shards', 'wait_for_nodes',
                  'wait_for_relocating_shards', 'wait_for_status')
    def health(self, index=None, params=None, headers=None):
        return {
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

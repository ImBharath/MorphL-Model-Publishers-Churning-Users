"""Google Analytics Reporting API V4 Connector for the MorphL project"""

# This connector is intended to run inside of a Docker container.
# Install Python dependencies:
# pip install google-auth google-api-python-client cassandra-driver
# In the same directory you will find in a .cql file all the prerequisite Cassandra table definitions.
# The following environment variables need to be set before executing this connector:
# DAY_OF_DATA_CAPTURE=2018-07-27
# MORPHL_SERVER_IP_ADDRESS
# MORPHL_CASSANDRA_PASSWORD
# KEY_FILE_LOCATION
# VIEW_ID

from json import dumps
from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class CassandraPersistance:
    def __init__(self):
        self.DAY_OF_DATA_CAPTURE = getenv('DAY_OF_DATA_CAPTURE')
        self.MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
        self.MORPHL_CASSANDRA_USERNAME = getenv('MORPHL_CASSANDRA_USERNAME')
        self.MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
        self.MORPHL_CASSANDRA_KEYSPACE = getenv('MORPHL_CASSANDRA_KEYSPACE')

        self.CASS_REQ_TIMEOUT = 3600.0

        self.auth_provider = PlainTextAuthProvider(
            username=self.MORPHL_CASSANDRA_USERNAME, password=self.MORPHL_CASSANDRA_PASSWORD)
        self.cluster = Cluster(
            [self.MORPHL_SERVER_IP_ADDRESS], auth_provider=self.auth_provider)
        self.session = self.cluster.connect(self.MORPHL_CASSANDRA_KEYSPACE)

        self.prep_stmts = {}
        self.level_sets = {}

    # Takes a dictionary of key lists sorted by type
    # report_types_by_level = {
    #   'client' : ['client_ids', 'destkop_device],
    #   'session' : ['geo_content', 'session_ids']
    # }

    def prepare_statements(self, report_types_by_level):

        templates_by_level = {
            'user_level': 'INSERT INTO ga_{} (client_id,day_of_data_capture, json_meta, json_data) VALUES (?,?,?,?)',
            'session_level': 'INSERT INTO ga_{} (client_id,day_of_data_capture, session_id, json_meta,json_data) VALUES (?,?,?,?,?)',
            'hit_level': 'INSERT INTO ga_{} (client_id,day_of_data_capture, session_id, tz_time, json_meta, json_data) VALUES (?,?,?,?,?,?)',
        }

        for level, list_of_report_types in report_types_by_level.items():
            if (list_of_report_types):
                for report_type in list_of_report_types:
                    self.prep_stmts[report_type] = self.session.prepare(
                        templates_by_level[level].format(report_type))

                self.level_sets[level] = set(list_of_report_types)

    def persist_dict_record(self, report_type, meta_dict, data_dict):
        raw_client_id = data_dict['dimensions'][0]
        client_id = raw_client_id if raw_client_id.startswith(
            'GA') else 'UNKNOWN'

        json_meta = dumps(meta_dict)
        json_data = dumps(data_dict)

        if report_type in self.level_sets['user_level']:
            bind_list = [client_id, self.DAY_OF_DATA_CAPTURE,
                         json_meta, json_data]
            return {'cassandra_future': self.session.execute_async(self.prep_stmts[report_type], bind_list, timeout=self.CASS_REQ_TIMEOUT), 'client_id': client_id}

        if report_type in self.level_sets['session_level']:
            session_id = data_dict['dimensions'][1]
            bind_list = [client_id, self.DAY_OF_DATA_CAPTURE,
                         session_id, json_meta, json_data]

            return {'cassandra_future': self.session.execute_async(self.prep_stmts[report_type], bind_list, timeout=self.CASS_REQ_TIMEOUT), 'client_id': client_id, 'session_id': session_id}

        if report_type in self.level_sets['hit_level']:
            session_id = data_dict['dimensions'][1]
            tz_time = data_dict['dimensions'][2]
            bind_list = [client_id, self.DAY_OF_DATA_CAPTURE,
                         tz_time, session_id, json_meta, json_data]

            return {'cassandra_future': self.session.execute_async(self.prep_stmts[report_type], bind_list, timeout=self.CASS_REQ_TIMEOUT), 'client_id': client_id, 'session_id': session_id}

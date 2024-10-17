# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import confluent_kafka as ck
from functools import partial, reduce
from enum import Enum
import time
import threading
import json
import random
import re
import requests
import socket
import time
import random
from typing import Any, Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.errors import TimeoutError
from ducktape.mark import matrix
from keycloak import KeycloakOpenID
from rptest.clients.default import DefaultClient
from rptest.clients.kcl import KCL
from rptest.clients.python_librdkafka import PythonLibrdkafka
from rptest.clients.rpk import RpkTool, RpkException
from rptest.services import tls
from rptest.services.admin import Admin, RoleMember
from rptest.services.cluster import cluster
from rptest.services import redpanda
from rptest.services.keycloak import DEFAULT_REALM, KeycloakService
from rptest.services.ocsf_server import OcsfServer
from rptest.services.redpanda import AUDIT_LOG_ALLOW_LIST, LoggingConfig, MetricSamples, MetricsEndpoint, PandaproxyConfig, RedpandaServiceBase, SchemaRegistryConfig, SecurityConfig, TLSProvider
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.cluster_config_test import wait_for_version_sync
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import expect_exception, wait_until, wait_until_result
from rptest.utils.mode_checks import skip_fips_mode
from rptest.utils.rpk_config import read_redpanda_cfg
from rptest.utils.schema_registry_utils import Mode, get_subjects, put_mode
from urllib.parse import urlparse


class AuthorizationMatch(str, Enum):
    ACL = 'acl'
    RBAC = 'rbac'


class StatusID(int, Enum):
    UNKNOWN = 0
    SUCCESS = 1
    FAILURE = 2
    OTHER = 99


class ClassUID(int, Enum):
    FILE_SYSTEM_ACTIVITY = 1001,
    KERNEL_EXTENSION_ACTIVITY = 1002,
    KERNEL_ACTIVITY = 1003,
    MEMORY_ACTIVITY = 1004,
    MODULE_ACTIVITY = 1005,
    SCHEDULED_JOB_ACTIVITY = 1006,
    PROCESS_ACTIVITY = 1007,
    SECURITY_FINDING = 2001,
    ACCOUNT_CHANGE = 3001,
    AUTHENTICATION = 3002,
    AUTHORIZE_SESSION = 3003,
    ENTITY_MANAGEMENT = 3004,
    USER_ACCESS_MANAGEMENT = 3005,
    GROUP_MANAGEMENT = 3006,
    NETWORK_ACTIVITY = 4001,
    HTTP_ACTIVITY = 4002,
    DNS_ACTIVITY = 4003,
    DHCP_ACTIVITY = 4004,
    RDP_ACTIVITY = 4005,
    SMB_ACTIVITY = 4006,
    SSH_ACTIVITY = 4007,
    FTP_ACTIVITY = 4008,
    EMAIL_ACTIVITY = 4009,
    NETWORK_FILE_ACTIVITY = 4010,
    EMAIL_FILE_ACTIVITY = 4011,
    EMAIL_URL_ACTIVITY = 4012,
    DEVICE_INVENTORY_INFO = 5001,
    DEVICE_CONFIG_STATE = 5002,
    WEB_RESOURCE_ACTIVITY = 6001,
    APPLICATION_LIFECYCLE = 6002,
    API_ACTIVITY = 6003,
    WEB_RESOURCE_ACCESS_ACTIVITY = 6004


class MTLSProvider(TLSProvider):
    """
    Defines an mTLS provider
    """
    def __init__(self, tls):
        self.tls = tls

    @property
    def ca(self):
        return self.tls.ca

    def create_broker_cert(self, redpanda, node):
        assert node in redpanda.nodes
        return self.tls.create_cert(node.name)

    def create_service_client_cert(self, _, name):
        return self.tls.create_cert(socket.gethostname(),
                                    name=name,
                                    common_name=name)


class BaseTestItem:
    """Base test item

    """
    def __init__(self, name: str, generate_function, filter_function):
        """Creates BaseTestItem

        Parameters
        ----------
        name : str
            Name of the test

        generate_function: function pointer
            The function used to generate traffic

        filter_function: function pointer
            The function used to generate received audit messages.  Last argument
            of function must be for the records to parse
        """
        self.name = name
        self.generate_function = generate_function
        self.filter_function = filter_function

    def valid_count(self, count: int) -> bool:
        """Checks to see if the count is valid

        Parameters
        ----------
        count: int
            The count of items

        Returns
        -------
        bool
           True if count is valid
        """
        raise NotImplementedError("Base does not implement this")

    def desc(self) -> str:
        """Returns description of test

        Returns
        -------
        str
            The description of the test
        """
        raise NotImplementedError("Base does not implement this")


class AbsoluteTestItem(BaseTestItem):
    """Used to test if an exact count of messages appears
    """
    def __init__(self, name, generate_function, filter_function, count):
        """Creates an AbsoluteTestItem

        Parameters
        ----------
        name : str
            Name of the test

        generate_function: function pointer
            The function used to generate traffic

        filter_function: function pointer
            The function used to generate received audit messages.  Last argument
            of function must be for the records to parse

        count: int
            The expected count
        """
        super(AbsoluteTestItem, self).__init__(name, generate_function,
                                               filter_function)
        self.count = count

    def valid_count(self, count: int) -> bool:
        return count == self.count

    def desc(self) -> str:
        return f'{self.count}'


class RangeTestItem(BaseTestItem):
    """Test item that expects the count in a range
    """
    def __init__(self, name, generate_function, filter_function, min, max):
        """Creates a RangeTestItem

        Expects the count of items to be [min, max]

        Parameters
        ----------
        name : str
            Name of the test

        generate_function: function pointer
            The function used to generate traffic

        filter_function: function pointer
            The function used to generate received audit messages.  Last argument
            of function must be for the records to parse

        min: int
            The minimum expected count of messages

        max: int
            The maximum expected count of messages
        """
        super(RangeTestItem, self).__init__(name, generate_function,
                                            filter_function)

        assert min <= max
        self.min = min
        self.max = max

    def valid_count(self, count: int) -> bool:
        return self.min <= count <= self.max

    def desc(self) -> str:
        return f'[{self.min}, {self.max}]'


class AuditLogConfig:
    """Configuration for the audit log system"""
    def __init__(self,
                 enabled: bool = True,
                 num_partitions: int = 8,
                 event_types=['management', 'admin']):
        """Initializes the config

        Parameters
        ----------
        enabled: bool, default=True
            Whether or not system is enabled

        num_partitions: int, default=8
            Number of partitions to create

        event_types: [str], default=['management']
            The event types to start with enabled
        """
        self.enabled = enabled
        self.num_partitions = num_partitions
        self.event_types = event_types

    def to_conf(self) -> {str, str}:
        """Converts conf to dict

        Returns
        -------
        {str, str}
            Key,value dictionary of configs
        """
        return {
            'audit_enabled': self.enabled,
            'audit_log_num_partitions': self.num_partitions,
            'audit_enabled_event_types': self.event_types
        }


class AuditLogTestSecurityConfig(SecurityConfig):
    def __init__(self,
                 admin_cert: Optional[tls.Certificate] = None,
                 user_creds: Optional[tuple[str, str, str]] = None,
                 user_cert: Optional[tls.Certificate] = None):
        super(AuditLogTestSecurityConfig, self).__init__()
        self._user_creds = user_creds
        self._user_cert = user_cert
        self._admin_cert = admin_cert

        if (self._user_creds is not None):
            assert self._user_cert is None and self._admin_cert is None, "Cannot set certs and password"
            self.enable_sasl = True
            self.kafka_enable_authorization = True
            self.endpoint_authn_method = 'sasl'
        elif (self._user_cert is not None or self._admin_cert is not None):
            assert self._user_cert is not None and self._admin_cert is not None, "Must set both certs"
            self.enable_sasl = False
            self.kafka_enable_authorization = True
            self.endpoint_authn_method = 'mtls_identity'
            self.require_client_auth = True

    @staticmethod
    def default_credentials():
        username = 'username'
        password = 'password'
        algorithm = 'SCRAM-SHA-256'
        return AuditLogTestSecurityConfig(user_creds=(username, password,
                                                      algorithm))

    def check_configuration(self):
        """Used by test harness to ensure auth is sufficent for audit logging
        """
        return self._user_creds is not None or (self._user_cert is not None and
                                                self._admin_cert is not None)

    @property
    def admin_cert(self) -> Optional[tls.Certificate]:
        return self._admin_cert

    @property
    def user_creds(self) -> Optional[tuple[str, str, str]]:
        return self._user_creds

    @property
    def user_cert(self) -> Optional[tls.Certificate]:
        return self._user_cert


class AuditLogTestBase(RedpandaTest):
    """Base test object for testing the audit logs"""
    audit_log = "_redpanda.audit_log"
    kafka_rpc_service_name = "kafka rpc protocol"
    admin_audit_svc_name = "Redpanda Admin HTTP Server"
    sr_audit_svc_name = "Redpanda Schema Registry Service"

    def __init__(
            self,
            test_context,
            audit_log_config: AuditLogConfig = AuditLogConfig(),
            log_config: LoggingConfig = LoggingConfig(
                'info', logger_levels={'auditing': 'trace'}),
            security: AuditLogTestSecurityConfig = AuditLogTestSecurityConfig.
        default_credentials(),
            audit_log_client_config: Optional[redpanda.AuditLogConfig] = None,
            extra_rp_conf=None,
            **kwargs):
        assert (security.check_configuration()
                ), "No auth enabled, test harness misconfigured"
        self.audit_log_config = audit_log_config

        self.extra_rp_conf = self.audit_log_config.to_conf()
        if extra_rp_conf is not None:
            self.extra_rp_conf = self.extra_rp_conf | extra_rp_conf
        self.log_config = log_config
        self.security = security
        self.audit_log_client_config = audit_log_client_config

        if self.security.mtls_identity_enabled():
            self.extra_rp_conf['kafka_mtls_principal_mapping_rules'] = [
                self.security.principal_mapping_rules
            ]

        super(AuditLogTestBase,
              self).__init__(test_context=test_context,
                             extra_rp_conf=self.extra_rp_conf,
                             log_config=self.log_config,
                             security=self.security,
                             audit_log_config=self.audit_log_client_config,
                             **kwargs)

        self.rpk = self.get_rpk()
        self.super_rpk = self.get_super_rpk()
        self.admin = Admin(self.redpanda,
                           auth=(self.redpanda.SUPERUSER_CREDENTIALS[0],
                                 self.redpanda.SUPERUSER_CREDENTIALS[1]))
        self.ocsf_server = OcsfServer(test_context)

    def get_rpk_credentials(self, username: str, password: str,
                            mechanism: str) -> RpkTool:
        """Creates an RpkTool with username & password
        """
        return RpkTool(self.redpanda,
                       username=username,
                       password=password,
                       sasl_mechanism=mechanism)

    def get_rpk(self) -> RpkTool:
        """Creates a regular instance of rpk
        """
        if self.security.sasl_enabled():
            return self.get_rpk_credentials(
                username=self.security.user_creds[0],
                password=self.security.user_creds[1],
                mechanism=self.security.user_creds[2])
        elif self.security.mtls_identity_enabled():
            return self.get_rpk_tls(self.security.user_cert)
        else:
            return RpkTool(self.redpanda)

    def get_rpk_tls(self, tls_cert: tls.Certificate) -> RpkTool:
        return RpkTool(self.redpanda, tls_cert=tls_cert, tls_enabled=True)

    def get_super_rpk(self) -> RpkTool:
        """Creates an RPK with superuser credentials
        """
        if self.security.sasl_enabled():
            return self.get_rpk_credentials(
                username=self.redpanda.SUPERUSER_CREDENTIALS[0],
                password=self.redpanda.SUPERUSER_CREDENTIALS[1],
                mechanism=self.redpanda.SUPERUSER_CREDENTIALS[2])
        elif self.security.mtls_identity_enabled():
            return self.get_rpk_tls(self.security.admin_cert)
        else:
            return RpkTool(self.redpanda)

    def setUp(self):
        """Initializes the Redpanda node and waits for audit log to be present
        """
        super().setUp()
        if self.security.sasl_enabled():
            self.super_rpk.sasl_create_user(self.security.user_creds[0],
                                            self.security.user_creds[1],
                                            self.security.user_creds[2])
        self.ocsf_server.start()
        self.logger.debug(
            f'Running OCSF Server Version {self.ocsf_server.get_api_version(None)}'
        )
        self.wait_for_audit_log()

    def wait_for_audit_log(self):
        """Waits for audit log to appear in the list of topics
        """
        self.logger.debug("Checking for existance of audit log")

        def _wait_for_audit_log(timeout_sec: int):
            wait_until(lambda: self.audit_log in self.super_rpk.list_topics(),
                       timeout_sec=timeout_sec,
                       backoff_sec=2)

        if self.audit_log_config.enabled:
            _wait_for_audit_log(timeout_sec=10)

    def _modify_cluster_config(self, upsert):
        patch_result = self.admin.patch_cluster_config(upsert=upsert)
        wait_for_version_sync(self.admin, self.redpanda,
                              patch_result['config_version'])

    def modify_audit_event_types(self, events: [str]):
        """Modifies the current audited events
        """
        self._modify_cluster_config({'audit_enabled_event_types': events})

    def modify_audit_excluded_topics(self, topics: [str]):
        """
        Modifies list of excluded topics
        """
        self._modify_cluster_config({'audit_excluded_topics': topics})

    def modify_audit_excluded_principals(self, principals: [str]):
        """
        Modifies list of excluded principals
        """
        self._modify_cluster_config({'audit_excluded_principals': principals})

    def change_max_buffer_size_per_shard(self, new_size: int):
        """
        Modifies the audit_queue_max_buffer_size_per_shard configuration
        """
        self._modify_cluster_config(
            {'audit_queue_max_buffer_size_per_shard': new_size})

    def modify_audit_enabled(self, enabled: bool):
        """
        Modifies value of audit_enabled
        """
        self._modify_cluster_config({'audit_enabled': enabled})

    def modify_node_config(self, node, update_fn, skip_readiness_check=True):
        """Modifies the current node configuration, restarts the node for
        changes to take effect
        """
        node_cfg = read_redpanda_cfg(node)
        self.redpanda.logger.debug(f"Existing node cfg: {node_cfg}")
        new_node_cfg = update_fn(node_cfg)

        # Restart the node with the modified cfg, maybe skip readiness check as access
        # to the health monitor will be blocked since error within auditing is detected
        self.redpanda.stop_node(node, timeout=10, forced=True)
        self.redpanda.start_node(node,
                                 override_cfg_params=new_node_cfg,
                                 skip_readiness_check=skip_readiness_check)

    @staticmethod
    def aggregate_count(records):
        """Aggregate count of records by checking for 'count' field
        """
        def combine(acc, x):
            return acc + (1 if 'count' not in x else x['count'])

        return reduce(combine, records, 0)

    @staticmethod
    def api_resource_match(expected_api_op, resource_entry, service_name,
                           record):
        return record['class_uid'] == 6003 and record['api']['service'][
            'name'] == service_name and record['api'][
                'operation'] == expected_api_op and resource_entry in record[
                    'resources']

    @staticmethod
    def multi_api_resource_match(expected: list[dict[str, dict[str, str]]],
                                 service_name, record):
        for items in expected:
            for expected_api_op, resource_entry in items.items():
                if AuditLogTestBase.api_resource_match(expected_api_op,
                                                       resource_entry,
                                                       service_name, record):
                    return True

        return False

    @staticmethod
    def api_match(expected_api_op, service_name, record):
        return record['class_uid'] == 6003 and record['api']['service'][
            'name'] == service_name and record['api'][
                'operation'] == expected_api_op

    @staticmethod
    def execute_command_ignore_error(fn):
        try:
            fn()
        except Exception:
            pass
        finally:
            pass

    def get_rpk_consumer(self, topic, offset='oldest') -> RpkConsumer:

        username = None
        password = None
        mechanism = None
        tls_cert = None

        if self.security.sasl_enabled():
            (username, password,
             mechanism) = self.redpanda.SUPERUSER_CREDENTIALS
        elif self.security.mtls_identity_enabled():
            tls_cert = self.security.admin_cert

        return RpkConsumer(self.test_context,
                           self.redpanda,
                           topic,
                           offset=offset,
                           username=username,
                           password=password,
                           mechanism=mechanism,
                           tls_cert=tls_cert,
                           tls_enabled=self.security.mtls_identity_enabled())

    def get_ck_producer(self) -> ck.Producer:
        config_opts = {
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '1'
        }

        if self.security.sasl_enabled():
            (username, password,
             mechanism) = self.redpanda.SUPERUSER_CREDENTIALS
            config_opts['sasl.username'] = username
            config_opts['sasl.password'] = password
            config_opts['sasl.mechanism'] = mechanism
            config_opts['security.protocol'] = 'SASL_PLAINTEXT'
        elif self.security.mtls_identity_enabled():
            config_opts['ssl.key.location'] = self.security.admin_cert.key
            config_opts[
                'ssl.certificate.location'] = self.security.admin_cert.crt
            config_opts['ssl.ca.location'] = self.security.admin_cert.ca.crt

        return ck.Producer(config_opts)

    def read_all_from_audit_log(self,
                                filter_fn,
                                stop_cond,
                                timeout_sec: int = 60,
                                backoff_sec: int = 1):
        """Reads all messages from the audit log

        Parameters
        ----------
        filter_fn:
            The function used to filter messages.  Last argument must accept
            a list of records

        stop_cond:
            The function to use to check to stop.  Last argument must accept
            a list of records

        timeout_sec: int, default=30,
            How long to wait

        backoff_sec: int, default=1
            Backoff

        Returns
        -------
        [str]
            List of records as json objects
        """
        class MessageMapper():
            def __init__(self, logger, filter_fn, stop_cond, ocsf_server):
                self.logger = logger
                self.records = []
                self.filter_fn = filter_fn
                self.stop_cond = stop_cond
                self.ocsf_server = ocsf_server
                self.next_offset_ingest = 0

            def ingest(self, records):
                new_records = records[self.next_offset_ingest:]
                if len(new_records) == 0:
                    self.logger.debug(
                        f"No new records observed, currently have read {len(records)} records so far"
                    )
                    return
                self.next_offset_ingest = len(records)
                new_records = [json.loads(msg['value']) for msg in new_records]
                self.logger.info(f"Ingested: {len(new_records)} records")
                self.logger.debug(f'Ingested records:')
                for rec in new_records:
                    self.logger.debug(f'{rec}')
                    self.ocsf_server.validate_schema(rec)
                    if self.filter_fn(rec):
                        self.logger.debug(f'Selected {rec}')
                        self.records.append(rec)
                    else:
                        self.logger.debug(f'DID NOT SELECT {rec}')

            def is_finished(self):
                return stop_cond(self.records)

        mapper = MessageMapper(self.redpanda.logger, filter_fn, stop_cond,
                               self.ocsf_server)
        self.redpanda.logger.debug("Starting audit_log consumer...")
        consumer = self.get_rpk_consumer(topic=self.audit_log, offset='oldest')
        consumer.start()

        def predicate():
            mapper.ingest(consumer.messages)
            return mapper.is_finished()

        try:
            wait_until(predicate,
                       timeout_sec=timeout_sec,
                       backoff_sec=backoff_sec)
        except Exception as e:
            actual = self.aggregate_count(mapper.records)
            self.logger.error(
                f"Failed waiting on records, observed: {actual} records")
            raise e
        finally:
            consumer.stop()
            consumer.free()
            self.redpanda.logger.debug("audit_log consumer has stopped")
        return mapper.records

    def find_matching_record(self, filter_fn, valid_check_fn, desc):
        """Finds matching records and validate the count

        Parameters
        ----------
        filter_fn:
            The filter function to select records.  Last argument must accept
            list of records

        valid_check_fn:
            Function used to check if count is valid.  Last argument must accept
            an integer

        desc:
            Function used to describe the test

        Returns
        -------

        Matched records
        """
        stop_cond = lambda records: valid_check_fn(
            self.aggregate_count(records))
        return self.read_all_from_audit_log(filter_fn=filter_fn,
                                            stop_cond=stop_cond)


class AuditLogTestsAppLifecycle(AuditLogTestBase):
    """Validates that app lifecycle events occur
    """
    def __init__(self, test_context):
        super(AuditLogTestsAppLifecycle,
              self).__init__(test_context=test_context,
                             audit_log_config=AuditLogConfig(event_types=[]),
                             log_config=LoggingConfig('info',
                                                      logger_levels={
                                                          'auditing': 'trace',
                                                          'kafka/client':
                                                          'trace',
                                                      }))

    @staticmethod
    def is_lifecycle_match(feature: Optional[str], is_start: bool, record):
        expected_activity_id = 3 if is_start else 4

        return record['class_uid'] == 6002 and record[
            'activity_id'] == expected_activity_id and (
                (feature is not None and 'feature' in record['app']
                 and record['app']['feature']['name'] == feature) or
                (feature is None and 'feature' not in record['app']))

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_app_lifecycle(self):
        _ = self.find_matching_record(
            partial(AuditLogTestsAppLifecycle.is_lifecycle_match,
                    "Audit System", True),
            lambda record_count: record_count == 3,
            "Single redpanda audit start event per node")

        _ = self.find_matching_record(
            partial(AuditLogTestsAppLifecycle.is_lifecycle_match, None,
                    True), lambda record_count: record_count == 3,
            "Single redpanda start event per node")

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_drain_on_audit_disabled(self):
        """
        Test the drain on disabling of audit is working properly by setting audit_enabled
        to False and asserting that the stop application_lifecycle event is observed"""

        self._modify_cluster_config({'audit_enabled': False})

        self.stop_per_node = set({})

        # Ensure that one erraneous node returning many stop events doesn't allow the test
        # to pass by filtering only by unique events. Use the node id provided in the record
        # payload to do so.
        def filter_unique_stop_events(record):
            is_match = AuditLogTestsAppLifecycle.is_lifecycle_match(
                "Audit System", False, record)
            if is_match:
                nodeid = record['app']['uid']
                if nodeid not in self.stop_per_node:
                    self.stop_per_node.add(nodeid)
                    return True
            return False

        _ = self.find_matching_record(
            filter_unique_stop_events, lambda record_count: record_count == 3,
            "Three more stop events observed per node")

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_recovery_mode(self):
        """
        Tests that audit logging does not start when in recovery mode
        """

        # Expect to find the audit system to come up
        _ = self.find_matching_record(
            partial(AuditLogTestsAppLifecycle.is_lifecycle_match,
                    "Audit System", True),
            lambda record_count: record_count == 3,
            "Single redpanda audit start event per node")
        # Change goes into effect next restart
        self.change_max_buffer_size_per_shard(1)
        self.modify_audit_event_types(['admin', 'authenticate'])

        # Restart and ensure we see the error message
        self.redpanda.restart_nodes(
            self.redpanda.nodes,
            override_cfg_params={"recovery_mode_enabled": True})
        wait_until(lambda: self.redpanda.search_log_any(
            'Redpanda is operating in recovery mode.  Auditing is disabled!'),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Did not find expected log statement")

        # Execute a few Admin API calls that would be normally audited
        # If everything is working, these should return true with
        # no issue
        for _ in range(0, 10):
            _ = self.admin.get_features()

        # Change goes into effect next restart
        self.change_max_buffer_size_per_shard(1024 * 1024)
        self.modify_audit_event_types([])
        self.redpanda.restart_nodes(
            self.redpanda.nodes,
            override_cfg_params={"recovery_mode_enabled": False})
        # Now we should see it 6 times, 3 times for initial boot, and 3 more times for this latest
        # boot.  Seeing >6 would mean auditing somehow worked while in recovery mode
        records = self.find_matching_record(
            partial(AuditLogTestsAppLifecycle.is_lifecycle_match,
                    "Audit System", True),
            lambda record_count: record_count >= 6,
            "Single redpanda audit start event per node")
        assert len(
            records) == 6, f'Expected 6 start up records, found {len(records)}'


class AuditLogTestAdminApi(AuditLogTestBase):
    """Validates that audit logs are generated from admin API
    """
    def __init__(self, test_context):
        super(AuditLogTestAdminApi,
              self).__init__(test_context=test_context,
                             audit_log_config=AuditLogConfig(num_partitions=1,
                                                             event_types=[]),
                             log_config=LoggingConfig('info',
                                                      logger_levels={
                                                          'auditing':
                                                          'trace',
                                                          'admin_api_server':
                                                          'trace'
                                                      }))

    @skip_fips_mode
    @cluster(num_nodes=4)
    def test_config_rejected(self):
        """
        Ensures that attempting to add _redpanda.audit_log to excluded topics will be
        rejected
        """
        # Should pass
        self.modify_audit_excluded_topics(['good'])
        try:
            self.modify_audit_excluded_topics(['good', self.audit_log])
            assert "This should have failed"
        except requests.HTTPError:
            pass

        try:
            self.modify_audit_excluded_topics(['this*is*a*bad*name'])
            assert "This should have failed"
        except requests.HTTPError:
            pass

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_audit_log_functioning(self):
        """
        Ensures that the audit log can be produced to when the audit_enabled()
        configuration option is set, and that the same actions do nothing
        when the option is unset. Furthermore verifies that the internal duplicate
        aggregation feature is working.
        """
        def is_api_match(matches, record):
            if record['class_uid'] == 6003 and record['dst_endpoint'][
                    'svc_name'] == self.admin_audit_svc_name:
                regex = re.compile(
                    "http:\/\/(?P<address>.*):(?P<port>\d+)\/v1\/(?P<handler>.*)"
                )
                string = record['http_request']['url']['url_string']
                match = regex.match(string)
                if match is None:
                    raise RuntimeError(f'Record out of spec: {record}')
                return match.group('handler') in matches
            else:
                return False

        def number_of_records_matching(filter_by, n_expected):
            filter_fn = partial(is_api_match, filter_by)

            stop_cond = lambda records: self.aggregate_count(records
                                                             ) >= n_expected
            records = self.read_all_from_audit_log(filter_fn, stop_cond)
            assert self.aggregate_count(
                records
            ) == n_expected, f"Expected: {n_expected}, Actual: {self.aggregate_count(records)}"
            return records

        self.modify_audit_event_types(['admin'])

        # The test override the default event type to 'heartbeat', therefore
        # any actions on the admin server should not result in audit msgs
        api_calls = {
            'features/license': self.admin.get_license,
            'cluster/health_overview': self.admin.get_cluster_health_overview
        }
        api_keys = api_calls.keys()
        call_apis = lambda: [fn() for fn in api_calls.values()]
        self.logger.debug("Starting 500 api calls with management enabled")
        for _ in range(0, 500):
            call_apis()
        self.logger.debug("Finished 500 api calls with management enabled")

        time.sleep(5)
        records = number_of_records_matching(api_keys, 1000)
        self.redpanda.logger.debug(f"records: {records}")

        # Remove management setting
        self.modify_audit_event_types(['heartbeat'])

        time.sleep(5)
        self.logger.debug("Started 500 api calls with management disabled")
        for _ in range(0, 500):
            call_apis()
        self.logger.debug("Finished 500 api calls with management disabled")
        _ = number_of_records_matching(api_keys, 1000)

    @skip_fips_mode
    @cluster(num_nodes=4)
    def test_audit_log_metrics(self):
        """
        Confirm that audit log metrics are present
        """
        def get_metrics_from_node(
            node: ClusterNode,
            patterns: list[str],
            endpoint: MetricsEndpoint = MetricsEndpoint.METRICS
        ) -> Optional[dict[str, MetricSamples]]:
            def get_metrics_from_node_sync(patterns: list[str]):
                samples = self.redpanda.metrics_samples(
                    patterns, [node], endpoint)
                success = samples is not None and set(
                    samples.keys()) == set(patterns)
                return success, samples

            try:
                return wait_until_result(
                    lambda: get_metrics_from_node_sync(patterns),
                    timeout_sec=2,
                    backoff_sec=.1)
            except TimeoutError as e:
                return None

        public_metrics = [
            "audit_last_event",
            "audit_errors_total",
        ]
        metrics = public_metrics + [
            "audit_buffer_usage_ratio",
            "audit_client_buffer_usage_ratio",
        ]

        for node in self.redpanda.nodes:
            samples = get_metrics_from_node(node, metrics)
            assert samples, f"Missing expected metrics from node {node.name}"
            assert set(samples.keys()) == set(
                metrics), f"Metrics incomplete: {samples.keys()}"

        for node in self.redpanda.nodes:
            samples = get_metrics_from_node(node, public_metrics,
                                            MetricsEndpoint.PUBLIC_METRICS)
            assert samples, f"Missing expected public metrics from node {node.name}"
            assert set(samples.keys()) == set(
                public_metrics), f"Public metrics incomplete: {samples.keys()}"

        # Remove management setting
        patch_result = self.admin.patch_cluster_config(
            upsert={'audit_enabled_event_types': ['heartbeat']})
        wait_for_version_sync(self.admin, self.redpanda,
                              patch_result['config_version'])


class AuditLogTestAdminAuthApi(AuditLogTestBase):
    """
    Validates auditing when auth is enabled on the
    Admin API
    """
    username = 'test'
    password = 'test12345'
    algorithm = 'SCRAM-SHA-256'

    ignored_user = 'ignored-test'
    ignored_pass = 'ignored-test'

    def __init__(self, test_context):
        super(AuditLogTestAdminAuthApi, self).__init__(
            test_context=test_context,
            audit_log_config=AuditLogConfig(
                num_partitions=1, event_types=['admin', 'authenticate']),
            log_config=LoggingConfig('info',
                                     logger_levels={
                                         'auditing': 'trace',
                                         'admin_api_server': 'trace'
                                     }),
            security=AuditLogTestSecurityConfig(user_creds=(self.username,
                                                            self.password,
                                                            self.algorithm)))

    def setup_cluster(self):
        self._modify_cluster_config({'admin_api_require_auth': True})
        self.admin.create_user(self.username, self.password, self.algorithm)
        self.admin.create_user(self.ignored_user, self.ignored_pass,
                               self.algorithm)

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_excluded_principal(self):
        self.setup_cluster()
        self.modify_audit_excluded_principals([self.ignored_user])

        Admin(self.redpanda,
              auth=(self.username, self.password)).get_raft_recovery_status(
                  node=self.redpanda.nodes[0])
        Admin(self.redpanda,
              auth=(self.ignored_user,
                    self.ignored_pass)).get_raft_recovery_status(
                        node=self.redpanda.nodes[0])

        def match_api_user(endpoint, user, svc_name, record):
            if record['class_uid'] == 6003 and record['dst_endpoint'][
                    'svc_name'] == svc_name:
                regex = re.compile(
                    "http:\/\/(?P<address>.*):(?P<port>\d+)\/v1\/(?P<handler>.*)"
                )
                url_string = record['http_request']['url']['url_string']
                match = regex.match(url_string)
                if match and match.group('handler') == endpoint and record[
                        'actor']['user']['name'] == user:
                    return True
            return False

        records = self.find_matching_record(
            lambda record:
            match_api_user("raft/recovery/status", self.username, self.
                           admin_audit_svc_name, record),
            lambda record_count: record_count >= 1, 'raft recory normal user')
        assert len(records) == 1, f'Expected one record found {len(records)}'

        try:
            records = self.find_matching_record(
                lambda record:
                match_api_user("raft/recovery/status", self.ignored_user, self.
                               admin_audit_svc_name, record),
                lambda record_count: record_count >= 1,
                'raft recovery ignored user',
            )
            assert len(
                records
            ) == 0, f'Expected to find zero records but found {len(records)}: {records}'
        except TimeoutError:
            pass


class AuditLogTestKafkaApi(AuditLogTestBase):
    """Validates that the Kafka API generates audit messages
    """
    def __init__(self, test_context):

        super(AuditLogTestKafkaApi,
              self).__init__(test_context=test_context,
                             audit_log_config=AuditLogConfig(num_partitions=1,
                                                             event_types=[]),
                             log_config=LoggingConfig('info',
                                                      logger_levels={
                                                          'auditing': 'trace',
                                                          'kafka': 'trace'
                                                      }))

        (username, password, mechanism) = self.redpanda.SUPERUSER_CREDENTIALS
        self.kcl = KCL(self.redpanda,
                       username=username,
                       password=password,
                       sasl_mechanism=mechanism)
        self.default_client = DefaultClient(self.redpanda)

    @skip_fips_mode
    @cluster(num_nodes=4)
    def test_audit_topic_protections(self):
        """Validates audit topic protections
        """
        try:
            self.super_rpk.produce(self.audit_log, "key", "value")
            assert False, 'Rpk was successfully allowed to produce to the audit log'
        except RpkException as e:
            if 'TOPIC_AUTHORIZATION_FAILED' not in e.stderr:
                raise

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_excluded_topic(self):
        """
        Validates that no audit messages are created for topics that
        are in the excluded topic list
        """

        excluded_topic = 'excluded_topic'
        included_topic = 'included_topic'

        self.modify_audit_event_types(
            ['management', 'produce', 'consume', 'heartbeat', 'describe'])
        self.modify_audit_excluded_topics([excluded_topic])

        self.super_rpk.create_topic(topic=excluded_topic)
        self.super_rpk.create_topic(topic=included_topic)

        self.super_rpk.produce(topic=excluded_topic, key="test", msg="msg")
        self.super_rpk.produce(topic=included_topic, key="test", msg="msg")

        _ = self.super_rpk.consume(topic=excluded_topic, n=1)
        _ = self.super_rpk.consume(topic=included_topic, n=1)

        def records_containing_topic(topic: str, record):
            return record['class_uid'] == 6003 and record['api']['service'][
                'name'] == self.kafka_rpc_service_name and {
                    'name': topic,
                    'type': 'topic'
                } in record['resources']

        records = self.find_matching_record(
            lambda record: records_containing_topic(included_topic, record),
            lambda record_count: record_count >= 1,
            "Should contain the included topic")

        assert len(
            records
        ) > 0, f'Did not receive any audit records for topic {included_topic}'

        try:
            records = self.find_matching_record(
                lambda record: records_containing_topic(
                    excluded_topic, record),
                lambda record_count: record_count > 0,
                "Should not contain any of these records")
            assert len(
                records
            ) == 0, f'Found {len(records)} records containing {excluded_topic}'
            assert "find_matching_record did not fail as expected"
        except TimeoutError:
            pass

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_management(self):
        """Validates management messages
        """

        topic_name = 'test_mgmt_audit'

        def alter_partition_reassignments_with_kcl(
                kcl: KCL, topics: dict[str, dict[int, list[int]]]):

            kcl.alter_partition_reassignments(topics=topics)

        def alter_config_with_kcl(kcl: KCL, values: dict[str, Any],
                                  incremental: bool):
            kcl.alter_broker_config(values, incremental)

        tests = [
            AbsoluteTestItem(
                f'Create Topic {topic_name}',
                lambda: self.super_rpk.create_topic(topic=topic_name),
                partial(self.api_resource_match, "create_topics", {
                    "name": f"{topic_name}",
                    "type": "topic"
                }, self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'Add partitions to {topic_name}',
                lambda: self.super_rpk.add_partitions(topic=topic_name,
                                                      partitions=3),
                partial(self.api_resource_match, "create_partitions", {
                    "name": f"{topic_name}",
                    "type": "topic"
                }, self.kafka_rpc_service_name), 1),
            RangeTestItem(
                f'Attempt group offset delete',
                lambda: self.execute_command_ignore_error(
                    partial(self.super_rpk.offset_delete, "fake",
                            {topic_name: [0]})),
                partial(self.api_resource_match, "offset_delete", {
                    "name": "fake",
                    "type": "group"
                }, self.kafka_rpc_service_name), 1,
                5),  # expect five because rpk will retry
            RangeTestItem(
                f'Attempting delete records for {topic_name}',
                lambda: self.execute_command_ignore_error(
                    partial(self.super_rpk.trim_prefix, topic_name, 0)),
                partial(self.api_resource_match, "delete_records", {
                    "name": f"{topic_name}",
                    "type": "topic"
                }, self.kafka_rpc_service_name), 1, 3),
            AbsoluteTestItem(
                f'Delete Topic {topic_name}',
                lambda: self.super_rpk.delete_topic(topic=topic_name),
                partial(self.api_resource_match, "delete_topics", {
                    "name": f"{topic_name}",
                    "type": "topic"
                }, self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'Create ACL', lambda: self.super_rpk.sasl_allow_principal(
                    principal="test",
                    operations=["all"],
                    resource="topic",
                    resource_name="test",
                    username=self.redpanda.SUPERUSER_CREDENTIALS[0],
                    password=self.redpanda.SUPERUSER_CREDENTIALS[1],
                    mechanism=self.redpanda.SUPERUSER_CREDENTIALS[2]),
                partial(
                    self.api_resource_match, "create_acls", {
                        "name": "create acl",
                        "type": "acl_binding",
                        "data": {
                            "resource_type": "topic",
                            "resource_name": "test",
                            "pattern_type": "literal",
                            "acl_principal": "type {user} name {test}",
                            "acl_host": "{{any_host}}",
                            "acl_operation": "all",
                            "acl_permission": "allow"
                        }
                    }, self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'Delete ACL',
                lambda: self.super_rpk.delete_principal(principal="test",
                                                        operations=["all"],
                                                        resource="topic",
                                                        resource_name="test"),
                partial(
                    self.api_resource_match, "delete_acls", {
                        "name": "delete acl",
                        "type": "acl_binding_filter",
                        "data": {
                            "resource_type": "topic",
                            "resource_name": "test",
                            "acl_principal": "type {user} name {test}",
                            "acl_operation": "all",
                            "acl_permission": "allow"
                        }
                    }, self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'Delete group test',
                lambda: self.execute_command_ignore_error(
                    partial(self.super_rpk.group_delete, "test")),
                partial(self.api_resource_match, "delete_groups", {
                    "name": "test",
                    "type": "group"
                }, self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'Alter Partition Reassignments',
                lambda: self.execute_command_ignore_error(
                    partial(alter_partition_reassignments_with_kcl, self.kcl,
                            {topic_name: {
                                1: [0]
                            }})),
                partial(self.api_resource_match,
                        "alter_partition_reassignments", {
                            "name": topic_name,
                            "type": "topic"
                        }, self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'Alter Config (not-incremental)',
                lambda: self.execute_command_ignore_error(
                    partial(alter_config_with_kcl, self.kcl, {
                        "log_message_timestamp_type": "CreateTime"
                    }, False)),
                partial(self.api_match, "alter_configs",
                        self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'Incremental Alter Config',
                lambda: self.execute_command_ignore_error(
                    partial(alter_config_with_kcl, self.kcl, {
                        "log_message_timestamp_type": "CreateTime"
                    }, True)),
                partial(self.api_match, "incremental_alter_configs",
                        self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'List ACLs (no item)', lambda: self.super_rpk.acl_list(),
                partial(self.api_match, "list_acls",
                        self.kafka_rpc_service_name), 0)
        ]

        # Enable management now
        self.logger.debug("Modifying event types")
        self.modify_audit_event_types(['management'])

        for test in tests:
            self.logger.info(f'Running test "{test.name}"')
            test.generate_function()
            _ = self.find_matching_record(test.filter_function,
                                          test.valid_count, test.desc())

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_produce(self):
        """Validates produce audit messages
        """

        topic_name = 'test_produce_audit'
        tx_topic_name = 'test_produce_tx_audit'

        self.super_rpk.create_topic(topic=topic_name, partitions=3)
        self.super_rpk.create_topic(topic=tx_topic_name, partitions=3)

        def transaction_generate():
            producer = self.get_ck_producer()
            producer.init_transactions()
            producer.begin_transaction()
            producer.produce(tx_topic_name, '0', '0', 1)
            producer.produce(tx_topic_name, '0', '1', 2)
            producer.flush()

        tests = [
            AbsoluteTestItem(
                f'Produce one message to {topic_name}', lambda: self.super_rpk.
                produce(topic_name, key='Test key', msg='Test msg'),
                partial(self.api_resource_match, "produce", {
                    "name": f'{topic_name}',
                    "type": "topic"
                }, self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'Produce two messages to {tx_topic_name}',
                lambda: transaction_generate(),
                partial(self.multi_api_resource_match, [{
                    "produce": {
                        "name": f'{tx_topic_name}',
                        "type": "topic"
                    }
                }, {
                    "produce": {
                        "name": "1",
                        "type": "transactional_id"
                    }
                }], self.kafka_rpc_service_name), 4)
        ]

        self.logger.debug("Modifying event types")
        self.modify_audit_event_types(['produce'])

        for test in tests:
            self.logger.info(f'Running test "{test.name}"')
            test.generate_function()
            _ = self.find_matching_record(test.filter_function,
                                          test.valid_count, test.desc())

    @skip_fips_mode
    @cluster(num_nodes=4, log_allow_list=AUDIT_LOG_ALLOW_LIST)
    def test_no_auth_enabled(self):
        """The expected behavior of the system when working with no auth
        enabled is to omit warning logs and prevent any messages from being
        enqueued, thus blocking all requests for which auditing is enabled for
        """
        stop_thread = False
        self.modify_audit_event_types(['admin'])

        def generate_async_audit_events():
            while stop_thread is not True:
                try:
                    _ = [
                        self.admin.get_license(node=node, timeout=1)
                        for node in self.redpanda.nodes
                    ]
                except Exception as _:
                    pass
                time.sleep(1)

        gen_event_thread = threading.Thread(target=generate_async_audit_events,
                                            args=())
        gen_event_thread.start()

        def modify_auth_method(method, listeners: [str], node_cfg):
            node_kafka_cfg = node_cfg['redpanda']['kafka_api']
            for l in listeners:
                listener = [e for e in node_kafka_cfg if e['name'] == l]
                assert len(listener) == 1, f'Expected listener {l}'
                listener = listener[0]
                assert 'authentication_method' in listener, f'Expected authentication_method in {l}'
                listener['authentication_method'] = method

            return node_cfg['redpanda']

        # Modify the node config to remove authentication on the listener of 9092
        node = self.redpanda.nodes[0]
        self.modify_node_config(node,
                                partial(modify_auth_method, 'none',
                                        ['dnslistener', 'iplistener']),
                                skip_readiness_check=True)

        # Observe that auditing is issuing warnings about misconfiguration
        exc = None
        try:
            audit_misconfig_warn = '.*Audit message rejected due to misconfigured authorization'
            wait_until(
                lambda: self.redpanda.search_log_any(audit_misconfig_warn),
                timeout_sec=30,
                backoff_sec=2)
        except Exception as e:
            exc = e
        finally:
            stop_thread = True
            gen_event_thread.join()

            # Reset the configuration to what it was for clean shutdown
            self.modify_node_config(node,
                                    partial(
                                        modify_auth_method,
                                        self.security.endpoint_authn_method,
                                        ['dnslistener', 'iplistener']),
                                    skip_readiness_check=False)

        if exc is not None:
            raise exc

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_consume(self):
        """
        Validates audit messages on consume
        """

        topic_name = 'test_consume_audit'

        def test_fetch_and_produce():
            consumer = self.get_rpk_consumer(topic_name)
            consumer.start()
            # Allow consumer to poll
            time.sleep(1)
            self.super_rpk.produce(topic_name, "key", "val")
            wait_until(lambda: consumer.message_count >= 1,
                       timeout_sec=10,
                       backoff_sec=1,
                       err_msg="Should have received at least one message")
            # Allow consumer to poll one more time
            consumer.stop()
            assert consumer.message_count == 1, f'Expected one message but got {consumer.message_count}'
            consumer.free()

        self.super_rpk.create_topic(topic=topic_name, partitions=1)

        self.modify_audit_event_types(['consume'])

        test_fetch_and_produce()

        records = self.find_matching_record(
            lambda record: self.api_resource_match("fetch", {
                "name": topic_name,
                "type": "topic"
            }, self.kafka_rpc_service_name, record),
            lambda record_count: record_count >= 1, "fetch request")

        self.logger.debug(f'Records received: {records}')

        # We expect at least one, but no more than two fetch authz events
        assert 1 <= len(
            records
        ) <= 2, f'Expected 1 or 2 fetch records, received {len(records)}'


class AuditLogTestKafkaAuthnApi(AuditLogTestBase):
    """Validates SASL/SCRAM authentication messages
    """
    username = 'test'
    password = 'test12345'
    algorithm = 'SCRAM-SHA-256'

    def __init__(self, test_context):
        super(AuditLogTestKafkaAuthnApi, self).__init__(
            test_context=test_context,
            audit_log_config=AuditLogConfig(num_partitions=1,
                                            event_types=['authenticate']),
            security=AuditLogTestSecurityConfig(user_creds=(self.username,
                                                            self.password,
                                                            self.algorithm)),
            log_config=LoggingConfig('info',
                                     logger_levels={
                                         'auditing': 'trace',
                                         'kafka': 'trace',
                                         'security': 'trace'
                                     }))

    def setup_cluster(self):
        self.admin.create_user(self.username, self.password, self.algorithm)
        self.super_rpk.sasl_allow_principal(
            principal=self.username,
            operations=['all'],
            resource='topic',
            resource_name="*",
            username=self.redpanda.SUPERUSER_CREDENTIALS[0],
            password=self.redpanda.SUPERUSER_CREDENTIALS[1],
            mechanism=self.redpanda.SUPERUSER_CREDENTIALS[2])

    @staticmethod
    def authn_filter_function(service_name, username: str, protocol_id: int,
                              protocol_name: Optional[str], record):
        return record['class_uid'] == 3002 and record['service'][
            'name'] == service_name and record['user'][
                'name'] == username and record[
                    'auth_protocol_id'] == protocol_id and (
                        protocol_name is not None and record['auth_protocol']
                        == protocol_name) and record['status_id'] == 1

    @staticmethod
    def authn_failure_filter_function(service_name, username: str,
                                      protocol_id: int,
                                      protocol_name: Optional[str],
                                      error_msg: str, record):
        return record['class_uid'] == 3002 and record['service'][
            'name'] == service_name and record['user'][
                'name'] == username and record[
                    'auth_protocol_id'] == protocol_id and (
                        protocol_name is not None
                        and record['auth_protocol'] == protocol_name
                    ) and record['status_id'] == 2 and record[
                        'status_detail'] == error_msg

    @staticmethod
    def authz_api_filter_function(service_name, username: str, record):
        return record['class_uid'] == 6003 and record['api']['service'][
            'name'] == service_name and record['actor']['user'][
                'name'] == username

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_excluded_principal(self):
        """
        Verifies that principals excluded will not generate audit messages
        """
        self.setup_cluster()
        user2 = "ignored_user"
        user2_pw = "ignored_user"
        user2_alg = "SCRAM-SHA-256"

        self.modify_audit_excluded_principals([user2])

        self.admin.create_user(user2, user2_pw, user2_alg)
        self.super_rpk.sasl_allow_principal(
            principal=user2,
            operations=['all'],
            resource='topic',
            resource_name='*',
            username=self.redpanda.SUPERUSER_CREDENTIALS[0],
            password=self.redpanda.SUPERUSER_CREDENTIALS[1],
            mechanism=self.redpanda.SUPERUSER_CREDENTIALS[2])

        user2_rpk = self.get_rpk_credentials(username=user2,
                                             password=user2_pw,
                                             mechanism=user2_alg)

        _ = self.rpk.list_topics()
        _ = user2_rpk.list_topics()

        def contains_principal(principal: str, record):
            if record['class_uid'] == 3002:
                return record['user']['name'] == principal
            elif record['class_uid'] == 6003:
                return record['actor']['user']['name'] == principal
            return False

        records = self.find_matching_record(
            lambda record: contains_principal(self.username, record),
            lambda record_count: record_count > 0,
            f'Should contain {self.username}')

        assert len(
            records
        ) > 0, f'Did not receive any audit messages for principal {self.username}'

        try:
            records = self.find_matching_record(
                lambda record: contains_principal(user2, record),
                lambda record_count: record_count > 0,
                f'Should not contain {user2}')

            # We may find the user _only if_ the user principal is used during an authz check
            # against the audit log.  (e.g. metadata request)
            for r in records:
                assert r[
                    'class_uid'] == 6003, f'Should not see any ignored users in class {r["class_uid"]}'
                assert {
                    "name": self.audit_log,
                    "type": "topic"
                } in r[
                    'resources'], f'Did not find {self.audit_log} topic in resources'
        except TimeoutError:
            pass

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_authn_messages(self):
        """Verifies that authentication messages are audited
        """
        self.setup_cluster()

        # Now attempt to get the topic list as the regular user
        user_rpk = self.get_rpk()

        _ = user_rpk.list_topics()

        records = self.read_all_from_audit_log(
            partial(self.authn_filter_function, self.kafka_rpc_service_name,
                    self.username, 99, "SASL-SCRAM"),
            lambda records: self.aggregate_count(records) >= 1)

        assert len(
            records) == 1, f"Expected only one record got {len(records)}"

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_authn_failure_messages(self):
        """Validates that failed authentication messages are audited
        """
        self.setup_cluster()

        user_rpk = self.get_rpk_credentials(username=self.username,
                                            password="WRONG",
                                            mechanism=self.algorithm)

        try:
            _ = user_rpk.list_topics()
            assert 'This should fail'
        except Exception:
            pass

        records = self.read_all_from_audit_log(
            partial(
                self.authn_failure_filter_function,
                self.kafka_rpc_service_name, self.username, 99, "SASL-SCRAM",
                'SASL authentication failed: security: Invalid credentials'),
            lambda records: self.aggregate_count(records) >= 1)

        assert len(
            records) == 1, f'Expected only one record, got {len(records)}'

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_no_audit_user_authn(self):
        """
        Validates that no audit user authz events occur, but authn
        events should
        """
        self.setup_cluster()
        self.modify_audit_event_types([
            'management', 'produce', 'consume', 'describe', 'heartbeat',
            'authenticate'
        ])

        _ = self.get_rpk_credentials(username=self.username,
                                     password=self.password,
                                     mechanism=self.algorithm).list_topics()

        authn_records = self.read_all_from_audit_log(
            partial(self.authn_filter_function, self.kafka_rpc_service_name,
                    "__auditing", 99, "SASL-SCRAM"),
            lambda records: self.aggregate_count(records) >= 1)

        assert len(
            authn_records
        ) >= 1, f"Expected at least one authn record for audit user, but got none"

        try:
            recs = self.read_all_from_audit_log(
                partial(self.authz_api_filter_function,
                        self.kafka_rpc_service_name, "__auditing"),
                lambda records: self.aggregate_count(records) >= 1,
                timeout_sec=5)
            assert f'Should not have received any authn from __auditing but received {len(recs)}'
        except TimeoutError:
            # Good!  Should not have seen any!
            pass


class AuditLogTestInvalidConfigBase(AuditLogTestBase):
    username = 'test'
    password = 'test12345'
    algorithm = 'SCRAM-SHA-256'
    """
    Tests situations where audit log client is not properly configured
    """
    def __init__(self,
                 test_context,
                 audit_log_config=AuditLogConfig(enabled=False,
                                                 num_partitions=1,
                                                 event_types=[]),
                 log_config=LoggingConfig('info',
                                          logger_levels={
                                              'auditing': 'trace',
                                              'kafka': 'trace',
                                              'security': 'trace'
                                          }),
                 **kwargs):
        self.test_context = test_context
        # The 'none' below will cause the audit log client to not be configured properly
        self._audit_log_client_config = redpanda.AuditLogConfig(
            listener_port=9192, listener_authn_method='none')
        self._audit_log_client_config.require_client_auth = False
        self._audit_log_client_config.enable_broker_tls = False

        super(AuditLogTestInvalidConfigBase, self).__init__(
            test_context=test_context,
            audit_log_config=audit_log_config,
            log_config=log_config,
            audit_log_client_config=self._audit_log_client_config,
            **kwargs)

    def setUp(self):
        super().setUp()
        self.admin.create_user(self.username, self.password, self.algorithm)
        self.get_super_rpk().acl_create_allow_cluster(self.username, 'All')
        # Following is important so the rest of ducktape functions correctly
        self.modify_audit_excluded_principals(['admin'])
        self.modify_audit_event_types(['authenticate'])
        self.modify_audit_enabled(True)
        # Waits for all audit clients to enter the same state where any attempt
        # to enqueue an event will be rejected because the client is misconfigured
        wait_until(lambda: self.redpanda.search_log_all(
            'error_code: illegal_sasl_state'),
                   timeout_sec=30,
                   backoff_sec=2,
                   err_msg="Did not see illegal_sasl_state error message")


class AuditLogTestInvalidConfig(AuditLogTestInvalidConfigBase):
    def __init__(self, test_context):
        super(AuditLogTestInvalidConfig, self).__init__(
            test_context=test_context,
            security=AuditLogTestSecurityConfig(user_creds=(self.username,
                                                            self.password,
                                                            self.algorithm)))

    @skip_fips_mode
    @cluster(num_nodes=4,
             log_allow_list=[
                 r'Failed to append authentication event to audit log',
                 r'Failed to audit.*'
             ])
    def test_invalid_config(self):
        """
        Test validates that the topic is failed to get created if audit
        system is not configured correctly.
        """
        try:
            self.get_rpk().create_topic('test')
            assert False, "Should not have created a topic"
        except RpkException as e:
            assert "audit system failure: BROKER_NOT_AVAILABLE" in str(
                e
            ), f'{str(e)} does not contain "audit system failure: BROKER_NOT_AVAILABLE"'


class AuditLogTestInvalidConfigMTLS(AuditLogTestInvalidConfigBase):
    """
    Tests situations where audit log client is not properly configured and mTLS enabled
    """
    def __init__(self, test_context):
        self.test_context = test_context
        self.tls = tls.TLSCertManager(self.logger)
        self.user_cert = self.tls.create_cert(socket.gethostname(),
                                              common_name=self.username,
                                              name='base_client')
        self.admin_user_cert = self.tls.create_cert(
            socket.gethostname(),
            common_name=RedpandaServiceBase.SUPERUSER_CREDENTIALS[0],
            name='admin_client')
        self._security_config = AuditLogTestSecurityConfig(
            admin_cert=self.admin_user_cert, user_cert=self.user_cert)
        self._security_config.tls_provider = MTLSProvider(self.tls)
        self._security_config.principal_mapping_rules = 'RULE:.*CN=(.*).*/$1/'

        super(AuditLogTestInvalidConfigMTLS,
              self).__init__(test_context=test_context,
                             security=self._security_config)

    @skip_fips_mode
    @cluster(
        num_nodes=4,
        log_allow_list=[
            r'Failed to append authentication event to audit log',
            r'Failed to audit.*',
            r'Failed to enqueue mTLS authentication event - audit log system error'
        ])
    def test_invalid_config_mtls(self):
        """
        Validates that mTLS authn is rejected when audit client is misconfigured.
        Also ensures there is no segfault: https://redpandadata.atlassian.net/browse/CORE-7245
        """
        try:
            self.get_rpk().create_topic('test')
            assert False, "Should not have created a topic"
        except RpkException as e:
            pass

        assert self.redpanda.search_log_any(
            'Failed to enqueue mTLS authentication event - audit log system error'
        )


class AuditLogTestKafkaTlsApi(AuditLogTestBase):
    """
    Tests that validate audit log messages for users authenticated via mTLS
    """
    username = 'test'
    password = 'test12345'
    algorithm = 'SCRAM-SHA-256'

    def __init__(self, test_context):
        self.test_context = test_context
        self.tls = tls.TLSCertManager(self.logger)

        self.user_cert = self.tls.create_cert(socket.gethostname(),
                                              common_name=self.username,
                                              name='base_client')
        self.admin_user_cert = self.tls.create_cert(
            socket.gethostname(),
            common_name=RedpandaServiceBase.SUPERUSER_CREDENTIALS[0],
            name='admin_client')

        self._security_config = AuditLogTestSecurityConfig(
            admin_cert=self.admin_user_cert, user_cert=self.user_cert)
        self._security_config.tls_provider = MTLSProvider(self.tls)
        self._security_config.principal_mapping_rules = 'RULE:.*CN=(.*).*/$1/'

        self._audit_log_client_config = redpanda.AuditLogConfig(
            listener_port=9192, listener_authn_method='sasl')

        self._audit_log_client_config.require_client_auth = False
        self._audit_log_client_config.enable_broker_tls = False

        super(AuditLogTestKafkaTlsApi, self).__init__(
            test_context=test_context,
            audit_log_config=AuditLogConfig(num_partitions=1,
                                            event_types=['authenticate']),
            security=self._security_config,
            log_config=LoggingConfig('info',
                                     logger_levels={
                                         'auditing': 'trace',
                                         'kafka': 'trace',
                                         'security': 'trace'
                                     }),
            audit_log_client_config=self._audit_log_client_config)

    def setup_cluster(self):
        self.admin.create_user(self.username, self.password, self.algorithm)

    @staticmethod
    def mtls_authn_filter_function(service_name: str, username: str,
                                   protocol_id: int,
                                   protocol_name: Optional[str], dn: str,
                                   record):
        return record['class_uid'] == 3002 and record['service'][
            'name'] == service_name and record['user'][
                'name'] == username and record[
                    'auth_protocol_id'] == protocol_id and (
                        protocol_name is not None and record['auth_protocol']
                        == protocol_name) and record[
                            'status_id'] == 1 and record['user']['uid'] == dn

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_mtls(self):
        """
        Verify that mTLS authn users generate correct audit log entries
        """
        self.setup_cluster()

        user_rpk = self.get_rpk()

        _ = user_rpk.list_topics()

        records = self.read_all_from_audit_log(
            partial(self.mtls_authn_filter_function,
                    self.kafka_rpc_service_name, self.username, 99, "mtls",
                    f"O=Redpanda,CN={self.username}"),
            lambda records: self.aggregate_count(records) >= 1)

        assert len(
            records) == 1, f'Expected only one record got {len(records)}'


class AuditLogTestOauth(AuditLogTestBase):
    """
    Tests that validate audit log messages for users authenticated via OAUTH
    """
    client_id = 'myapp'
    token_audience = 'account'
    example_topic = 'foo'

    def __init__(self, test_context):
        security = AuditLogTestSecurityConfig(
            user_creds=RedpandaServiceBase.SUPERUSER_CREDENTIALS)
        security.enable_sasl = True
        security.sasl_mechanisms = ['SCRAM']
        security.http_authentication = ['BASIC']
        # We'll only enable Oath once keycloak is up and running

        self.keycloak = KeycloakService(test_context)

        super(AuditLogTestOauth, self).__init__(
            test_context=test_context,
            audit_log_config=AuditLogConfig(num_partitions=1,
                                            event_types=['authenticate']),
            security=security,
            log_config=LoggingConfig('info',
                                     logger_levels={
                                         'auditing': 'trace',
                                         'kafka': 'trace',
                                         'security': 'trace'
                                     }))

    def setUp(self):
        super().setUp()

        kc_node = self.keycloak.nodes[0]
        try:
            self.keycloak.start_node(kc_node)
        except Exception as e:
            self.logger.error(f'{e}')
            self.keycloak.clean_node(kc_node)
            assert False, f'Keycloak failed to start: {e}'

        self.security.sasl_mechanisms += ['OAUTHBEARER']
        self.security.http_authentication += ['OIDC']

        self._modify_cluster_config({
            'oidc_discovery_url':
            self.keycloak.get_discovery_url(kc_node),
            "oidc_token_audience":
            self.token_audience,
            "sasl_mechanisms":
            self.security.sasl_mechanisms,
            "http_authentication":
            self.security.http_authentication,
        })

        self.keycloak.admin.create_user('norma',
                                        'despond',
                                        realm_admin=True,
                                        email='10086@sunset.blvd')
        self.keycloak.login_admin_user(kc_node, 'norma', 'despond')
        self.keycloak.admin.create_client(self.client_id)
        self.keycloak.admin.update_user(f'service-account-{self.client_id}',
                                        email='myapp@customer.com')

    @staticmethod
    def oidc_authn_filter_function(service_name: str, username: str,
                                   sub: Optional[str], record):
        return record['class_uid'] == 3002 and record['service'][
            'name'] == service_name and record[
                'auth_protocol_id'] == 6 and record['user'][
                    'name'] == username and (record['user']['uid'] == sub
                                             if sub is not None else True)

    @staticmethod
    def oidc_metadata_filter_function(service_name: str, topic: str,
                                      username: str, role: Optional[str],
                                      record):
        return record['class_uid'] == 6003 and record['api']['service'][
            'name'] == service_name and record['api'][
                'operation'] == 'metadata' and record.get('resources') and any(
                    resource['type'] == 'topic' and resource['name'] == topic
                    for resource in record.get('resources')
                ) and record['actor']['user']['name'] == username and (
                    record['actor']['user'].get('groups') == [{
                        'type': 'role',
                        'name': role
                    }] if role is not None else True)

    @skip_fips_mode
    @cluster(num_nodes=6)
    @matrix(authz_match=[AuthorizationMatch.ACL, AuthorizationMatch.RBAC])
    def test_kafka_oauth(self, authz_match):
        """
        Validate that authentication events using OAUTH in Kafka
        generate valid audit messages
        """
        self.modify_audit_event_types(['describe', 'authenticate'])
        kc_node = self.keycloak.nodes[0]
        self.super_rpk.create_topic(self.example_topic)
        service_user_id = self.keycloak.admin_ll.get_user_id(
            f'service-account-{self.client_id}')
        role = None
        if authz_match == AuthorizationMatch.ACL:
            _ = self.super_rpk.sasl_allow_principal(
                f'User:{service_user_id}', ['all'], 'topic',
                self.example_topic, self.redpanda.SUPERUSER_CREDENTIALS[0],
                self.redpanda.SUPERUSER_CREDENTIALS[1],
                self.redpanda.SUPERUSER_CREDENTIALS[2])
        elif authz_match == AuthorizationMatch.RBAC:
            role = 'all_topics'
            _ = self.super_rpk.sasl_allow_principal(
                f'RedpandaRole:{role}', ['all'], 'topic', self.example_topic,
                self.redpanda.SUPERUSER_CREDENTIALS[0],
                self.redpanda.SUPERUSER_CREDENTIALS[1],
                self.redpanda.SUPERUSER_CREDENTIALS[2])
            self.admin.update_role_members(
                role=role,
                add=[
                    RoleMember(RoleMember.PrincipalType.USER, service_user_id)
                ],
                create=True)

        cfg = self.keycloak.generate_oauth_config(kc_node, self.client_id)
        assert cfg.client_secret is not None, "client_secret is None"
        assert cfg.token_endpoint is not None, "token_endpoint is None"
        k_client = PythonLibrdkafka(self.redpanda,
                                    algorithm='OAUTHBEARER',
                                    oauth_config=cfg)
        producer = k_client.get_producer()

        producer.poll(0.0)
        expected_topics = set([self.example_topic])
        wait_until(lambda: set(producer.list_topics(timeout=5).topics.keys())
                   == expected_topics,
                   timeout_sec=5)

        records = self.read_all_from_audit_log(
            partial(self.oidc_authn_filter_function,
                    self.kafka_rpc_service_name, service_user_id,
                    service_user_id),
            lambda records: self.aggregate_count(records) >= 1)

        # There may exist multiple OAUTH entries but that could be due to the client
        # connecting to more than one node.  In this situation the number of records
        # should match a set of unique ips
        ip_set = set()
        [ip_set.add(r["dst_endpoint"]["ip"]) for r in records]

        assert len(records) == len(
            ip_set), f"Expected one record but received {len(records)}"

        records = self.read_all_from_audit_log(
            partial(self.oidc_metadata_filter_function,
                    self.kafka_rpc_service_name, self.example_topic,
                    service_user_id, role),
            lambda records: self.aggregate_count(records) >= 1)

        assert 1 == len(
            records), f"Expected one record but received {len(records)}"

    @skip_fips_mode
    @cluster(num_nodes=6)
    def test_admin_oauth(self):
        """
        Validate that authentication events using OAUTH in the Admin API
        generate valid audit messages
        """
        kc_node = self.keycloak.nodes[0]
        cfg = self.keycloak.generate_oauth_config(kc_node, self.client_id)
        token_endpoint_url = urlparse(cfg.token_endpoint)
        openid = KeycloakOpenID(
            server_url=
            f'{token_endpoint_url.scheme}://{token_endpoint_url.netloc}',
            client_id=cfg.client_id,
            client_secret_key=cfg.client_secret,
            realm_name=DEFAULT_REALM,
            verify=True)
        token = openid.token(grant_type='client_credentials')
        userinfo = openid.userinfo(token['access_token'])

        def check_cluster_status():
            response = requests.get(
                url=
                f'http://{self.redpanda.nodes[0].account.hostname}:9644/v1/status/ready',
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {token["access_token"]}'
                },
                timeout=5)
            return response.status_code == requests.codes.ok

        wait_until(check_cluster_status, timeout_sec=5)

        records = self.read_all_from_audit_log(
            partial(self.oidc_authn_filter_function, self.admin_audit_svc_name,
                    userinfo['sub'], None),
            lambda records: self.aggregate_count(records) >= 1)

        ip_set = set()
        [ip_set.add(r["dst_endpoint"]["ip"]) for r in records]

        assert len(records) == len(
            ip_set), f"Expected one record but received {len(records)}"


class AuditLogTestSchemaRegistry(AuditLogTestBase):
    """
    Validates schema registry auditing
    """

    username = 'test'
    password = 'test'
    algorithm = 'SCRAM-SHA-256'

    def __init__(self, test_context):
        sr_config = SchemaRegistryConfig()
        sr_config.authn_method = 'http_basic'
        super(AuditLogTestSchemaRegistry, self).__init__(
            test_context=test_context,
            audit_log_config=AuditLogConfig(
                num_partitions=1,
                event_types=['schema_registry', 'authenticate']),
            log_config=LoggingConfig('info',
                                     logger_levels={
                                         'auditing': 'trace',
                                         'pandaproxy': 'trace'
                                     }),
            schema_registry_config=sr_config)

    def match_authn_record(self, record, status_id: StatusID):
        if record['class_uid'] == ClassUID.AUTHENTICATION and record[
                'dst_endpoint']['svc_name'] == self.sr_audit_svc_name:
            self.logger.debug(f"Validating auth record: {record}")

        return record['class_uid'] == ClassUID.AUTHENTICATION and \
            record['dst_endpoint']['svc_name'] == self.sr_audit_svc_name and \
            record['user']['name'] == self.username and \
            record['status_id'] == status_id

    def match_api_record(self,
                         record,
                         endpoint,
                         status_id: Optional[StatusID] = None):
        if record['class_uid'] == ClassUID.API_ACTIVITY and \
            record['dst_endpoint']['svc_name'] == self.sr_audit_svc_name:
            self.logger.debug(f"Validating api activity record: {record}")

        if status_id and record.get('status_id', '') != status_id:
            return False

        if record['class_uid'] == ClassUID.API_ACTIVITY \
            and record['dst_endpoint']['svc_name'] == self.sr_audit_svc_name \
            and record['actor']['user']['name'] == self.username:
            regex = re.compile(
                "http:\/\/(?P<address>.*):(?P<port>\d+)\/(?P<handler>.*)")
            url_string = record['http_request']['url']['url_string']
            match = regex.match(url_string)
            if match and match.group('handler') == endpoint:
                return True

        return False

    def setup_cluster(self):
        self.admin.create_user(self.username, self.password, self.algorithm)

        # wait for user to propagate to nodes
        def user_exists():
            for node in self.redpanda.nodes:
                users = self.admin.list_users(node=node)
                if self.username not in users:
                    return False
            return True

        wait_until(user_exists, timeout_sec=10, backoff_sec=1)

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_sr_audit(self):
        self.setup_cluster()

        r = get_subjects(self.redpanda.nodes,
                         self.logger,
                         auth=(self.username, self.password))
        assert r.status_code == requests.codes.ok

        records = self.find_matching_record(
            lambda record: self.match_api_record(record, "subjects"),
            lambda record_count: record_count >= 1, 'sr get api call')

        assert self.aggregate_count(records) == 1, \
            f'Expected one record found {self.aggregate_count(records)}: {records}'

        _ = self.find_matching_record(
            lambda record: self.match_authn_record(record, StatusID.SUCCESS),
            lambda record_count: record_count == 1, 'authn attempt in sr')

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_sr_audit_bad_authn(self):
        # Not calling self.setup_cluster() here so the user does not exist
        r = get_subjects(self.redpanda.nodes,
                         self.logger,
                         auth=(self.username, self.password))
        assert r.json()['error_code'] == 40101

        _ = self.find_matching_record(
            lambda record: self.match_authn_record(record, StatusID.FAILURE),
            lambda record_count: record_count > 1, 'authn fail attempt in sr')

        with expect_exception(TimeoutError, lambda _: True):
            _ = self.find_matching_record(
                lambda record: self.match_authn_record(record, StatusID.SUCCESS
                                                       ),
                lambda record_count: record_count >= 1,
                'authn fail attempt in sr')

        with expect_exception(TimeoutError, lambda _: True):
            _ = self.find_matching_record(
                lambda record: self.match_api_record(record, "subjects"),
                lambda aggregate_count: aggregate_count >= 1, 'API call')

    @skip_fips_mode
    @cluster(num_nodes=5)
    def test_sr_audit_bad_authz(self):
        self.setup_cluster()

        r = put_mode(self.redpanda.nodes,
                     self.logger,
                     mode=Mode.READONLY,
                     auth=(self.username, self.password))
        assert r.json()['error_code'] == 403, f"Response: {r.json()}"

        _ = self.find_matching_record(
            lambda record: self.match_authn_record(record, StatusID.SUCCESS),
            lambda record_count: record_count >= 1, 'authz fail attempt in sr')

        with expect_exception(TimeoutError, lambda _: True):
            _ = self.find_matching_record(
                lambda record: self.match_authn_record(record, StatusID.FAILURE
                                                       ),
                lambda record_count: record_count >= 1,
                'authn fail attempt in sr')

        _ = self.find_matching_record(
            lambda record: self.match_api_record(record, "mode", StatusID.
                                                 FAILURE),
            lambda aggregate_count: aggregate_count >= 1, 'API call')

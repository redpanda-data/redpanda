# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from functools import partial, reduce
import json
import re
from typing import Any, Optional

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import ok_to_fail
from rptest.clients.rpk import RpkTool
from rptest.services import tls
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services import redpanda
from rptest.services.redpanda import LoggingConfig, MetricSamples, MetricsEndpoint, SecurityConfig
from rptest.services.rpk_consumer import RpkConsumer
from rptest.tests.cluster_config_test import wait_for_version_sync
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import wait_until, wait_until_result
from rptest.utils.audit_schemas import validate_audit_schema


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
                 event_types=['management']):
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

    @property
    def admin_cert(self) -> Optional[tls.Certificate]:
        return self._admin_cert

    @property
    def user_creds(self) -> Optional[tuple[str, str, str]]:
        return self._user_creds

    @property
    def user_cert(self) -> Optional[tls.Certificate]:
        return self._user_cert


class AuditLogTestsBase(RedpandaTest):
    """Base test object for testing the audit logs"""
    audit_log = "__audit_log"
    kafka_rpc_service_name = "kafka rpc protocol"
    admin_audit_svc_name = "Redpanda Admin HTTP Server"

    def __init__(
            self,
            test_context,
            audit_log_config: AuditLogConfig = AuditLogConfig(),
            log_config: LoggingConfig = LoggingConfig(
                'info', logger_levels={'auditing': 'trace'}),
            security: AuditLogTestSecurityConfig = AuditLogTestSecurityConfig(
            ),
            audit_log_client_config: Optional[redpanda.AuditLogConfig] = None):
        self.audit_log_config = audit_log_config
        self.extra_rp_conf = self.audit_log_config.to_conf()
        self.log_config = log_config
        self.security = security
        self.audit_log_client_config = audit_log_client_config

        if self.security.mtls_identity_enabled():
            self.extra_rp_conf['kafka_mtls_principal_mapping_rules'] = [
                self.security.principal_mapping_rules
            ]

        super(AuditLogTestsBase,
              self).__init__(test_context=test_context,
                             extra_rp_conf=self.extra_rp_conf,
                             log_config=self.log_config,
                             security=self.security,
                             audit_log_config=self.audit_log_client_config)

        self.rpk = self.get_rpk()
        self.super_rpk = self.get_super_rpk()
        self.admin = Admin(self.redpanda)

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

    def get_rpk_consumer(self, topic, offset) -> RpkConsumer:

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

    def read_all_from_audit_log(self,
                                filter_fn,
                                stop_cond,
                                start_offset: str = 'oldest',
                                timeout_sec: int = 30,
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
        
        start_offset: str, default='oldest'
            Starting offset for the consumer
            
        timeout_sec: int, default=30,
            How long to wait
            
        backoff_sec: int, default=1
            Backoff
            
        consumer: Optional[RpkTool], default=None
            The consumer to use, or None if to use a new one
        
        Returns
        -------
        [str]
            List of records as json objects
        """
        class MessageMapper():
            def __init__(self, logger, filter_fn, stop_cond):
                self.logger = logger
                self.records = []
                self.filter_fn = filter_fn
                self.stop_cond = stop_cond
                self.next_offset_ingest = 0

            def ingest(self, records):
                new_records = records[self.next_offset_ingest:]
                self.next_offset_ingest = len(records)
                new_records = [json.loads(msg['value']) for msg in new_records]
                self.logger.debug(f'Ingested records:')
                for rec in new_records:
                    self.logger.debug(f'{rec}')
                self.logger.info(f"Ingested: {len(new_records)} records")
                [validate_audit_schema(record) for record in new_records]
                self.records += [r for r in new_records if self.filter_fn(r)]

            def is_finished(self):
                return stop_cond(self.records)

        mapper = MessageMapper(self.redpanda.logger, filter_fn, stop_cond)
        consumer = self.get_rpk_consumer(topic=self.audit_log,
                                         offset=start_offset)
        consumer.start()

        def predicate():
            mapper.ingest(consumer.messages)
            return mapper.is_finished()

        wait_until(predicate, timeout_sec=timeout_sec, backoff_sec=backoff_sec)
        consumer.stop()
        consumer.free()
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
        records = self.read_all_from_audit_log(filter_fn=filter_fn,
                                               stop_cond=stop_cond)
        assert valid_check_fn(
            self.aggregate_count(records)
        ), f'Expected {desc}, Actual: {self.aggregate_count(records)}'
        return records


class AuditLogTestsAdminApi(AuditLogTestsBase):
    """Validates that audit logs are generated from admin API
    """
    def __init__(self, test_context):
        super(AuditLogTestsAdminApi,
              self).__init__(test_context=test_context,
                             log_config=LoggingConfig('info',
                                                      logger_levels={
                                                          'auditing':
                                                          'trace',
                                                          'admin_api_server':
                                                          'trace'
                                                      }))

    @cluster(num_nodes=4)
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
                match = regex.match(
                        record['http_request']['url']['url_string'])
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

        records = number_of_records_matching(api_keys, 1000)
        self.redpanda.logger.debug(f"records: {records}")

        # Remove management setting
        self.modify_audit_event_types(['heartbeat'])

        self.logger.debug("Started 500 api calls with management disabled")
        for _ in range(0, 500):
            call_apis()
        self.logger.debug("Finished 500 api calls with management disabled")
        _ = number_of_records_matching(api_keys, 1000)

    @ok_to_fail  # https://github.com/redpanda-data/redpanda/issues/14565
    @cluster(num_nodes=3)
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
                success = samples is not None
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
            assert sorted(samples.keys()) == sorted(
                metrics), f"Metrics incomplete: {samples.keys()}"

        for node in self.redpanda.nodes:
            samples = get_metrics_from_node(node, metrics,
                                            MetricsEndpoint.PUBLIC_METRICS)
            assert samples, f"Missing expected public metrics from node {node.name}"
            assert sorted(samples.keys()) == sorted(
                public_metrics), f"Public metrics incomplete: {samples.keys()}"

        # Remove management setting
        patch_result = self.admin.patch_cluster_config(
            upsert={'audit_enabled_event_types': ['heartbeat']})
        wait_for_version_sync(self.admin, self.redpanda,
                              patch_result['config_version'])


class AuditLogTestsKafkaApi(AuditLogTestsBase):
    """Validates that the Kafka API generates audit messages
    """
    def __init__(self, test_context):

        super(AuditLogTestsKafkaApi,
              self).__init__(test_context=test_context,
                             audit_log_config=AuditLogConfig(num_partitions=1,
                                                             event_types=[]),
                             log_config=LoggingConfig('info',
                                                      logger_levels={
                                                          'auditing': 'trace',
                                                          'kafka': 'trace'
                                                      }))

    @cluster(num_nodes=4)
    def test_management(self):
        """Validates management messages
        """

        topic_name = 'test_mgmt_audit'

        tests = [
            AbsoluteTestItem(
                f'Create Topic {topic_name}',
                lambda: self.rpk.create_topic(topic=topic_name),
                partial(self.api_resource_match, "create_topics", {
                    "name": f"{topic_name}",
                    "type": "topic"
                }, self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'Create ACL',
                lambda: self.rpk.allow_principal(principal="test",
                                                 operations=["all"],
                                                 resource="topic",
                                                 resource_name="test"),
                partial(
                    self.api_resource_match, "create_acls", {
                        "name": "create acl",
                        "type": "acl_binding",
                        "data": {
                            "resource_type": "topic",
                            "resource_name": "test",
                            "pattern_type": "literal",
                            "acl_principal": "{type user name test}",
                            "acl_host": "{{any_host}}",
                            "acl_operation": "all",
                            "acl_permission": "allow"
                        }
                    }, self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'Delete ACL',
                lambda: self.rpk.delete_principal(principal="test",
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
                            "acl_principal": "{type user name test}",
                            "acl_operation": "all",
                            "acl_permission": "allow"
                        }
                    }, self.kafka_rpc_service_name), 1),
            AbsoluteTestItem(
                f'List ACLs (no item)', lambda: self.rpk.acl_list(),
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

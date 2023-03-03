from rptest.services.cluster import cluster

from rptest.clients.types import TopicSpec
from rptest.services.mock_iam_roles_server import MockIamRolesServer
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST
from rptest.tests.e2e_shadow_indexing_test import EndToEndShadowIndexingBase
from rptest.util import produce_until_segments, wait_for_local_storage_truncate


class AWSRoleFetchTests(EndToEndShadowIndexingBase):
    def __init__(self, test_context, extra_rp_conf=None):
        self.iam_server = MockIamRolesServer(test_context,
                                             'aws_iam_role_mock.py')
        if not extra_rp_conf:
            extra_rp_conf = {}

        super().__init__(test_context,
                         extra_rp_conf,
                         environment={
                             'RP_SI_CREDS_API_ADDRESS':
                             self.iam_server.address,
                         })
        self.redpanda.add_extra_rp_conf(
            {'cloud_storage_credentials_source': 'aws_instance_metadata'})

    def setUp(self):
        self.iam_server.start()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.iam_server.stop()
        self.iam_server.wait()
        self.iam_server.clean()

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_write(self):
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        local_retention = 5 * self.segment_size
        self.kafka_tools.alter_topic_config(
            self.topic,
            {
                TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES:
                local_retention,
            },
        )
        wait_for_local_storage_truncate(redpanda=self.redpanda,
                                        topic=self.topic,
                                        target_bytes=local_retention)
        self.start_consumer()
        self.run_validation()

        # each broker makes two requests to server, one to get role and one for credentials
        assert self.num_brokers * 2 == len(
            self.iam_server.requests
        ), f'{self.num_brokers} and {len(self.iam_server.requests)}'
        for request in self.iam_server.requests:
            # We do not know the order of requests, but they will be one of the two paths allowed
            assert request['path'] in {
                '/latest/meta-data/iam/security-credentials/',
                '/latest/meta-data/iam/security-credentials/tomato'
            }
            assert request['method'] == 'GET'
            assert request['response_code'] == 200


class STSRoleFetchTests(EndToEndShadowIndexingBase):
    def __init__(self, test_context, extra_rp_conf=None):
        self.iam_server = MockIamRolesServer(test_context,
                                             'aws_iam_role_mock.py',
                                             mock_target='sts')
        if not extra_rp_conf:
            extra_rp_conf = {}

        self.token_path = '/tmp/token_file'
        self.role = 'tomato'
        self.token = 'token-tomato'

        super().__init__(test_context,
                         extra_rp_conf,
                         environment={
                             'RP_SI_CREDS_API_ADDRESS':
                             self.iam_server.address,
                             'AWS_ROLE_ARN': self.role,
                             'AWS_WEB_IDENTITY_TOKEN_FILE': self.token_path,
                         })
        self.redpanda.add_extra_rp_conf(
            {'cloud_storage_credentials_source': 'sts'})

        for node in self.redpanda.nodes:
            node.account.create_file(self.token_path, self.token)

    def setUp(self):
        self.iam_server.start()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.iam_server.stop()
        self.iam_server.wait()
        self.iam_server.clean()

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_write(self):
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        local_retention = 5 * self.segment_size
        self.kafka_tools.alter_topic_config(
            self.topic,
            {TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES: local_retention},
        )
        wait_for_local_storage_truncate(self.redpanda,
                                        self.topic,
                                        target_bytes=local_retention)
        self.start_consumer()
        self.run_validation()

        # each broker makes one request to server
        assert self.num_brokers == len(self.iam_server.requests)
        for request in self.iam_server.requests:
            assert request['path'] == '/'
            assert request['method'] == 'POST'
            assert f'RoleArn={self.role}' in request['payload']
            assert f'WebIdentityToken={self.token}' in request['payload']
            assert request['response_code'] == 200


class ShortLivedCredentialsTests(EndToEndShadowIndexingBase):
    def __init__(self, test_context, extra_rp_conf=None):
        self.iam_server = MockIamRolesServer(test_context,
                                             'aws_iam_role_mock.py',
                                             mock_target='sts',
                                             ttl_sec=5)
        if not extra_rp_conf:
            extra_rp_conf = {}

        self.token_path = '/tmp/token_file'
        self.role = 'tomato'
        self.token = 'token-tomato'

        super().__init__(test_context,
                         extra_rp_conf,
                         environment={
                             'RP_SI_CREDS_API_ADDRESS':
                             self.iam_server.address,
                             'AWS_ROLE_ARN': self.role,
                             'AWS_WEB_IDENTITY_TOKEN_FILE': self.token_path,
                         })
        self.redpanda.add_extra_rp_conf(
            {'cloud_storage_credentials_source': 'sts'})

        for node in self.redpanda.nodes:
            node.account.create_file(self.token_path, self.token)

    def setUp(self):
        self.iam_server.start()
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.iam_server.stop()
        self.iam_server.wait()
        self.iam_server.clean()

    @cluster(num_nodes=6, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_short_lived_credentials(self):
        self.start_producer()
        produce_until_segments(
            redpanda=self.redpanda,
            topic=self.topic,
            partition_idx=0,
            count=10,
        )

        local_retention = 5 * self.segment_size
        self.kafka_tools.alter_topic_config(
            self.topic,
            {TopicSpec.PROPERTY_RETENTION_LOCAL_TARGET_BYTES: local_retention},
        )
        wait_for_local_storage_truncate(self.redpanda,
                                        self.topic,
                                        target_bytes=local_retention)
        self.start_consumer()
        self.run_validation()

        # each broker makes multiple requests to server as token is short-lived
        assert self.num_brokers < len(self.iam_server.requests)
        for request in self.iam_server.requests:
            assert request['path'] == '/'
            assert request['method'] == 'POST'
            assert f'RoleArn={self.role}' in request['payload']
            assert f'WebIdentityToken={self.token}' in request['payload']
            assert request['response_code'] == 200

from collections import defaultdict

from rptest.clients.rpk import RpkTool
from rptest.services.cluster import cluster

from rptest.clients.types import TopicSpec
from rptest.services.mock_iam_roles_server import MockIamRolesServer
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST, CloudStorageType, SISettings, get_cloud_storage_type
from rptest.tests.e2e_shadow_indexing_test import EndToEndShadowIndexingBase
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import produce_until_segments, wait_for_local_storage_truncate
from ducktape.utils.util import wait_until
from ducktape.mark import matrix, ignore


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

        assert self.num_brokers * 3 == len(
            self.iam_server.requests
        ), f'{self.num_brokers} and {len(self.iam_server.requests)}'
        calls = defaultdict(lambda: 0)
        for request in self.iam_server.requests:
            # We do not know the order of requests, but they will be one of the two paths allowed
            assert request['path'] in {
                '/latest/api/token',
                '/latest/meta-data/iam/security-credentials/',
                '/latest/meta-data/iam/security-credentials/tomato'
            }, f'unexpected path for {request}'
            calls[request['method']] += 1
            assert request[
                'response_code'] == 200, f'unexpected status for {request}'
        assert calls[
            'GET'] == self.num_brokers * 2, f'unexpected calls {calls}'
        assert calls['PUT'] == self.num_brokers, f'unexpected calls {calls}'


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


class AKSRoleFetchTests(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        self.iam_server = MockIamRolesServer(test_context,
                                             'aws_iam_role_mock.py',
                                             mock_target='aks')
        self.token_path = '/tmp/token_file'
        self.token = 'token'
        self.client_id = 'tomato'
        self.tenant_id = 'token-tomato'
        self.endpoint_url = 'endpoint.url'

        self.si_settings = SISettings(test_context,
                                      cloud_storage_max_connections=5,
                                      fast_uploads=True)

        super().__init__(test_context,
                         *args,
                         si_settings=self.si_settings,
                         extra_rp_conf={'log_segment_size': '1048576'},
                         environment={
                             'RP_SI_CREDS_API_ADDRESS':
                             self.iam_server.address,
                             'AZURE_CLIENT_ID': self.client_id,
                             'AZURE_TENANT_ID': self.tenant_id,
                             'AZURE_FEDERATED_TOKEN_FILE': self.token_path,
                             'AZURE_AUTHORITY_HOST': self.endpoint_url,
                         },
                         **kwargs)

        for node in self.redpanda.nodes:
            node.account.create_file(self.token_path, self.token)
        # need to set this after __init__ to overwrite SISettings
        self.redpanda.add_extra_rp_conf(
            {'cloud_storage_credentials_source': 'azure_aks_oidc_federation'})

    def setUp(self):
        self.iam_server.start()
        self.redpanda.start()

    def tearDown(self):
        super().tearDown()
        self.iam_server.stop()
        self.iam_server.wait()
        self.iam_server.clean()

    @ignore
    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_basic(self, cloud_storage_type):
        """
        verify that this configuration will fetch a credential from the mock server and use it to upload to Azure
        """
        if cloud_storage_type != CloudStorageType.ABS:
            return
        RpkTool(self.redpanda).create_topic(topic="panda-aks-smoketest",
                                            partitions=3,
                                            config={
                                                'redpanda.remote.read': 'true',
                                                'redpanda.remote.write': 'true'
                                            })
        for i in range(3):
            produce_until_segments(
                redpanda=self.redpanda,
                topic="panda-aks-smoketest",
                partition_idx=i,
                count=5,
            )

        wait_until(
            lambda: len(self.iam_server.requests) > 0,
            timeout_sec=300,
            backoff_sec=1,
            err_msg=lambda:
            f"failed to collect enough requests to the Mock Server. {len(self.iam_server.requests)}"
        )

        req = self.iam_server.requests[-1]
        assert req['path'] == '/token-tomato/oauth2/v2.0/token' and req[
            'method'] == 'POST' and f"client_id={self.client_id}" in req[
                'payload'] and f"client_assertion={self.token}" in req[
                    'payload'], f"request not conforming to specs {self.iam_server.requests}"


class AzureVMRoleFetchTests(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        self.iam_server = MockIamRolesServer(test_context,
                                             'aws_iam_role_mock.py',
                                             mock_target='azure_vm')
        self.client_id = 'tomato'
        self.si_settings = SISettings(test_context,
                                      cloud_storage_max_connections=5,
                                      fast_uploads=True)

        super().__init__(test_context,
                         *args,
                         si_settings=self.si_settings,
                         extra_rp_conf={'log_segment_size': '1048576'},
                         environment={
                             'RP_SI_CREDS_API_ADDRESS':
                             self.iam_server.address,
                         },
                         **kwargs)

        # need to set this after __init__ to overwrite SISettings
        self.redpanda.add_extra_rp_conf({
            'cloud_storage_credentials_source':
            'azure_vm_instance_metadata',
            'cloud_storage_azure_managed_identity_id':
            self.client_id
        })

    def setUp(self):
        self.iam_server.start()
        self.redpanda.start()

    def tearDown(self):
        super().tearDown()
        self.iam_server.stop()
        self.iam_server.wait()
        self.iam_server.clean()

    @ignore
    @cluster(num_nodes=2)
    @matrix(cloud_storage_type=get_cloud_storage_type())
    def test_basic(self, cloud_storage_type):
        """
        verify that this configuration will fetch a credential from the mock server and use it to upload to Azure
        """
        if cloud_storage_type != CloudStorageType.ABS:
            return
        RpkTool(self.redpanda).create_topic(topic="panda-avm-smoketest",
                                            partitions=3,
                                            config={
                                                'redpanda.remote.read': 'true',
                                                'redpanda.remote.write': 'true'
                                            })
        for i in range(3):
            produce_until_segments(
                redpanda=self.redpanda,
                topic="panda-avm-smoketest",
                partition_idx=i,
                count=5,
            )

        wait_until(
            lambda: len(self.iam_server.requests) > 0,
            timeout_sec=300,
            backoff_sec=1,
            err_msg=lambda:
            f"failed to collect enough requests to the Mock Server. {len(self.iam_server.requests)}"
        )

        req = self.iam_server.requests[-1]
        assert req['path'].startswith(
            '/metadata/identity/oauth2/token'
        ) and req['method'] == 'GET' and f"client_id={self.client_id}" in req[
            'path'], f"request not conforming to specs {self.iam_server.requests}"


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

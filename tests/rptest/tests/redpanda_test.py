# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os

from ducktape.tests.test import Test
from rptest.services.redpanda import RedpandaService
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.default import DefaultClient
from rptest.util import Scale


class RedpandaTest(Test):
    """
    Base class for tests that use the Redpanda service.
    """

    # List of topics to be created automatically when the cluster starts. Each
    # topic is defined by an instance of a TopicSpec.
    topics = []

    GLOBAL_S3_ACCESS_KEY = "s3_access_key"
    GLOBAL_S3_SECRET_KEY = "s3_secret_key"
    GLOBAL_S3_REGION_KEY = "s3_region"

    def __init__(self,
                 test_context,
                 num_brokers=None,
                 extra_rp_conf=dict(),
                 enable_pp=False,
                 enable_sr=False,
                 si_settings=None,
                 **kwargs):
        """
        Any trailing keyword arguments are passed through to the
        RedpandaService constructor.
        """
        super(RedpandaTest, self).__init__(test_context)
        self.scale = Scale(test_context)
        self.si_settings = si_settings

        if num_brokers is None:
            # Default to a 3 node cluster if sufficient nodes are available, else
            # a single node cluster.  This is just a default: tests are welcome
            # to override constructor to pass an explicit size.  This logic makes
            # it convenient to mix 3 node and 1 node cases in the same class, by
            # just modifying the @cluster node count per test.
            if test_context.cluster.available().size() >= 3:
                num_brokers = 3
            else:
                num_brokers = 1

        cloud_storage_access_key = test_context.globals.get(
            self.GLOBAL_S3_ACCESS_KEY, None)
        cloud_storage_secret_key = test_context.globals.get(
            self.GLOBAL_S3_SECRET_KEY, None)
        cloud_storage_region = test_context.globals.get(
            self.GLOBAL_S3_REGION_KEY, None)

        # Enable S3 if AWS creds were given at globals
        if cloud_storage_access_key and cloud_storage_secret_key:
            if self.si_settings is not None:
                self.logger.info("Running on AWS S3, setting credentials")
                self.si_settings.cloud_storage_access_key = cloud_storage_access_key
                self.si_settings.cloud_storage_secret_key = cloud_storage_secret_key
                self.si_settings.endpoint_url = None  # None so boto auto-gens the endpoint url
                self.si_settings.cloud_storage_disable_tls = False  # SI will fail to create archivers if tls is disabled
                self.si_settings.cloud_storage_region = cloud_storage_region
                self.si_settings.cloud_storage_api_endpoint_port = 443
        else:
            self.logger.debug(
                'No AWS credentials supplied, assuming minio defaults')

        self.redpanda = RedpandaService(test_context,
                                        num_brokers,
                                        extra_rp_conf=extra_rp_conf,
                                        enable_pp=enable_pp,
                                        enable_sr=enable_sr,
                                        si_settings=self.si_settings,
                                        **kwargs)
        self._client = DefaultClient(self.redpanda)

    @property
    def topic(self):
        """
        Return the name of the auto-created initial topic. Accessing this
        property requires exactly one initial topic be configured.
        """
        assert len(self.topics) == 1
        return self.topics[0].name

    @property
    def debug_mode(self):
        """
        Useful for tests that want to change behaviour when running on
        the much slower debug builds of redpanda, which generally cannot
        keep up with significant quantities of data or partition counts.
        """
        return os.environ.get('BUILD_TYPE', None) == 'debug'

    @property
    def ci_mode(self):
        """
        Useful for tests that want to dynamically degrade/disable on low-resource
        developer environments (e.g. laptops) but apply stricter checks in CI.
        """
        return os.environ.get('CI', None) != 'false'

    @property
    def s3_client(self):
        return self.redpanda.s3_client

    def setUp(self):
        self.redpanda.start()
        self._create_initial_topics()

    def client(self):
        return self._client

    def _create_initial_topics(self):
        config = self.redpanda.security_config()
        user = config.get("sasl_plain_username")
        passwd = config.get("sasl_plain_password")
        client = KafkaCliTools(self.redpanda, user=user, passwd=passwd)
        for spec in self.topics:
            self.logger.debug(f"Creating initial topic {spec}")
            client.create_topic(spec)

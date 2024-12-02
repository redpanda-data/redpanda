# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import time
from rptest.clients.default import DefaultClient
from rptest.clients.rpk import RpkTool, TopicSpec
from rptest.context import cloud_storage
from rptest.services.cluster import cluster

from rptest.services.polaris_catalog import PolarisCatalog, wait_until
from rptest.services.redpanda import RedpandaService
from rptest.services.tls import TLSCertManager
from rptest.tests.crl_test import RedpandaTest
from rptest.tests.polaris_catalog_test import PolarisCatalogTest
import polaris.catalog

from polaris.management.api.polaris_default_api import PolarisDefaultApi
from polaris.management.models.create_catalog_request import CreateCatalogRequest
from polaris.management.models.catalog_properties import CatalogProperties
from polaris.management.models.create_principal_request import CreatePrincipalRequest
from polaris.management.models.catalog import Catalog

from polaris.management.models.file_storage_config_info import FileStorageConfigInfo
from polaris.management.models.grant_principal_role_request import GrantPrincipalRoleRequest
from polaris.management.models.create_principal_role_request import CreatePrincipalRoleRequest
from polaris.management.models.create_catalog_role_request import CreateCatalogRoleRequest
from polaris.management.models.grant_catalog_role_request import GrantCatalogRoleRequest
from polaris.management.models.catalog_role import CatalogRole
from polaris.management.models.catalog_grant import CatalogGrant
from polaris.management.models.add_grant_request import AddGrantRequest
from polaris.management.models.principal_role import PrincipalRole
from polaris.management.models.catalog_privilege import CatalogPrivilege
from polaris.management.models.principal import Principal
from pyiceberg.catalog import load_catalog
from rptest.tests.datalake.utils import supported_storage_types
from ducktape.mark import matrix
from pyiceberg.catalog.rest import CLIENT_SECRET
from pyiceberg.catalog.rest import CLIENT_ID

from rptest.tests.redpanda_test import SISettings


class PolarisCatalogSmokeTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):

        super(PolarisCatalogSmokeTest,
              self).__init__(test_ctx,
                             num_brokers=3,
                             extra_rp_conf={},
                             si_settings=SISettings(test_ctx),
                             *args,
                             **kwargs)
        self.polaris = None
        self.cert_manager = TLSCertManager(self.logger)

    """
    Validates if the polaris catalog is accessible from ducktape tests harness
    """

    # defer starting all services to the test body
    def setUp(self):
        pass

    def _start_redpanda(self, catalog_prefix, client_id, client_secret,
                        with_tls):
        self.redpanda._extra_rp_conf.update({
            "iceberg_enabled":
            "true",
            "iceberg_catalog_type":
            "rest",
            "iceberg_rest_catalog_endpoint":
            self.polaris.catalog_url,
            "iceberg_rest_catalog_client_id":
            client_id,
            "iceberg_rest_catalog_client_secret":
            client_secret,
            "iceberg_catalog_commit_interval_ms":
            10000,
            "iceberg_rest_catalog_prefix":
            catalog_prefix
        })
        if with_tls:
            self.redpanda._extra_rp_conf.update({
                "iceberg_rest_catalog_trust_file":
                RedpandaService.TLS_CA_CRT_FILE,
            })
            for n in self.redpanda.nodes:
                n.account.mkdirs(
                    os.path.dirname(RedpandaService.TLS_CA_CRT_FILE))
                n.account.copy_to(self.cert_manager.ca.crt,
                                  RedpandaService.TLS_CA_CRT_FILE)

        self.redpanda.start()

    def _initialize_catalog(self, catalog_name, principal_name):
        polaris_api = PolarisDefaultApi(self.polaris.management_client())
        catalog = Catalog(
            type="INTERNAL",
            name=catalog_name,
            properties=CatalogProperties(
                default_base_location=
                f"file://{PolarisCatalog.PERSISTENT_ROOT}/catalog_data",
                additional_properties={}),
            storageConfigInfo=FileStorageConfigInfo(
                storageType="FILE",
                allowed_locations=["file://{PolarisCatalog.PERSISTENT_ROOT}"]))

        # create a catalog
        polaris_api.create_catalog(CreateCatalogRequest(catalog=catalog))

        # create principal and grant the catalog role
        r = polaris_api.create_principal(
            CreatePrincipalRequest(principal=Principal(name=principal_name)))
        credentials = r.credentials
        principal_role_name = f"test-{principal_name}-role"
        catalog_role_name = f"test-catalog-{principal_name}-role"
        polaris_api.create_principal_role(
            CreatePrincipalRoleRequest(principalRole=PrincipalRole(
                name=principal_role_name)))

        polaris_api.create_catalog_role(
            catalog_name=catalog_name,
            create_catalog_role_request=CreateCatalogRoleRequest(
                catalogRole=CatalogRole(name=catalog_role_name)))
        for p in [
                CatalogPrivilege.CATALOG_MANAGE_ACCESS,
                CatalogPrivilege.CATALOG_MANAGE_CONTENT,
                CatalogPrivilege.CATALOG_MANAGE_METADATA,
                CatalogPrivilege.CATALOG_READ_PROPERTIES,
                CatalogPrivilege.CATALOG_WRITE_PROPERTIES,
                CatalogPrivilege.NAMESPACE_CREATE,
        ]:
            polaris_api.add_grant_to_catalog_role(
                catalog_name=catalog_name,
                catalog_role_name=catalog_role_name,
                add_grant_request=AddGrantRequest(
                    grant=CatalogGrant(type='catalog', privilege=p)))
        polaris_api.assign_principal_role(
            principal_name=principal_name,
            grant_principal_role_request=GrantPrincipalRoleRequest(
                principalRole=PrincipalRole(name=principal_role_name)))

        polaris_api.assign_catalog_role_to_principal_role(
            catalog_name=catalog_name,
            principal_role_name=principal_role_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(
                catalogRole=CatalogRole(name=catalog_role_name)))
        return credentials

    def get_client(self, catalog_name, client_id, client_secret):
        self.credentials = cloud_storage.Credentials.from_context(
            self.test_context)
        conf = dict()
        conf["uri"] = self.polaris.catalog_url
        conf["type"] = "rest"
        conf[CLIENT_ID] = client_id
        conf[CLIENT_SECRET] = client_secret

        if isinstance(self.credentials, cloud_storage.S3Credentials):
            conf["s3.endpoint"] = self.credentials.endpoint
            conf["s3.access-key-id"] = self.credentials.access_key
            conf["s3.secret-access-key"] = self.credentials.secret_key
            conf["s3.region"] = self.credentials.region
        elif isinstance(self.credentials,
                        cloud_storage.AWSInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials,
                        cloud_storage.ABSSharedKeyCredentials):
            # Legancy pyiceberg https://github.com/apache/iceberg-python/issues/866
            conf["adlfs.account-name"] = self.credentials.account_name
            conf["adlfs.account-key"] = self.credentials.account_key
            # Modern pyiceberg https://github.com/apache/iceberg-python/issues/866
            conf["adls.account-name"] = self.credentials.account_name
            conf["alds.account-key"] = self.credentials.account_key
        else:
            raise ValueError(
                f"Unsupported credential type: {type(self.credentials)}")

        return load_catalog(catalog_name, **conf)

    def _create_polaris_service(self, with_tls):

        self.polaris = PolarisCatalog(
            ctx=self.test_context,
            node=None,
            tls=self.cert_manager if with_tls else None)
        self.polaris.start()

    @cluster(num_nodes=4)
    @matrix(cloud_storage_type=supported_storage_types(),
            with_tls=[True, False])
    def test_connecting_to_catalog(self, cloud_storage_type, with_tls):
        """The very basic test checking interaction with polaris catalog
        """
        self._create_polaris_service(with_tls)
        catalog_name = "ducky-catalog"
        principal_name = "test-user"
        credentials = self._initialize_catalog(catalog_name, principal_name)

        self._start_redpanda(catalog_prefix=catalog_name,
                             client_id=credentials.client_id,
                             client_secret=credentials.client_secret,
                             with_tls=with_tls)
        catalog_properties = {
            "uri": f"{self.polaris.catalog_url}",
            'type': "rest",
            'scope': "PRINCIPAL_ROLE:ALL",
            "credential":
            f"{credentials.client_id}:{credentials.client_secret}",
            "warehouse": catalog_name,
        }

        if with_tls:
            catalog_properties.update(
                {"ssl": {
                    "cabundle": self.cert_manager.ca.crt
                }})
        self.logger.info(f"catalog_properties: {catalog_properties}")
        py_catalog = load_catalog(catalog_name, **catalog_properties)

        py_catalog.create_namespace_if_not_exists("redpanda")

        namespaces = py_catalog.list_namespaces()

        assert "redpanda" in [n[0] for n in namespaces]
        topic = TopicSpec(partition_count=1, replication_factor=1)
        client = DefaultClient(self.redpanda)
        client.create_topic(topic)
        client.alter_topic_config(topic.name, TopicSpec.PROPERTY_ICEBERG_MODE,
                                  "key_value")
        rpk = RpkTool(self.redpanda)
        for i in range(10):
            rpk.produce(topic.name, "key", f"value-{i}")

        def _table_created():
            tables = py_catalog.list_tables("redpanda")
            self.logger.info(f"tables: {tables}")
            return len(tables) == 1 and tables[0][1] == topic.name

        wait_until(
            _table_created,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Error waiting for table to be created in Polaris catalog")

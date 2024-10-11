# Copyright 2020 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster

from rptest.services.polaris_catalog import PolarisCatalog
from rptest.tests.polaris_catalog_test import PolarisCatalogTest
import polaris.catalog

from polaris.management.api.polaris_default_api import PolarisDefaultApi
from polaris.management.models.create_catalog_request import CreateCatalogRequest
from polaris.management.models.catalog_properties import CatalogProperties
from polaris.management.models.create_principal_request import CreatePrincipalRequest
from polaris.management.models.catalog import Catalog
from polaris.management.models.storage_config_info import StorageConfigInfo
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


class PolarisCatalogSmokeTest(PolarisCatalogTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(PolarisCatalogSmokeTest, self).__init__(test_ctx,
                                                      num_brokers=1,
                                                      *args,
                                                      extra_rp_conf={},
                                                      **kwargs)

    """
    Validates if the polaris catalog is accessible from ducktape tests harness
    """

    @cluster(num_nodes=2)
    def test_creating_catalog(self):
        """The very basic test checking interaction with polaris catalog
        """
        polaris_api = PolarisDefaultApi(self.polaris.management_client())
        catalog = Catalog(
            type="INTERNAL",
            name="test-catalog",
            properties=CatalogProperties(
                default_base_location=
                f"file://{PolarisCatalog.PERSISTENT_ROOT}/catalog_data",
                additional_properties={}),
            storageConfigInfo=StorageConfigInfo(storageType="FILE"))

        polaris_api.create_catalog(CreateCatalogRequest(catalog=catalog))
        resp = polaris_api.list_catalogs()

        assert len(resp.catalogs) == 1
        assert resp.catalogs[0].name == "test-catalog"

    @cluster(num_nodes=2)
    def test_connectiong_to_catalog(self):
        """The very basic test checking interaction with polaris catalog
        """
        polaris_api = PolarisDefaultApi(self.polaris.management_client())
        catalog_name = "test-catalog"
        principal_name = "test-user"
        principal_role_name = "test-user-role"
        catalog_role_name = "test-catalog-role"
        catalog = Catalog(
            type="INTERNAL",
            name=catalog_name,
            properties=CatalogProperties(
                default_base_location=
                f"file://{PolarisCatalog.PERSISTENT_ROOT}/catalog_data",
                additional_properties={}),
            storageConfigInfo=StorageConfigInfo(storageType="FILE"))

        # create a catalog
        polaris_api.create_catalog(CreateCatalogRequest(catalog=catalog))

        # create principal and grant the catalog role
        r = polaris_api.create_principal(
            CreatePrincipalRequest(principal=Principal(name=principal_name)))
        credentials = r.credentials

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

        py_catalog = load_catalog(
            "test-catalog", **{
                "uri": f"{self.polaris.catalog_url}",
                'type': "rest",
                'scope': "PRINCIPAL_ROLE:ALL",
                "credential":
                f"{credentials.client_id}:{credentials.client_secret}",
                "warehouse": "test-catalog",
            })

        py_catalog.create_namespace_if_not_exists("test_namespace")

        namespaces = py_catalog.list_namespaces()

        assert "test_namespace" in [n[0] for n in namespaces]

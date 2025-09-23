# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import abc
from enum import Enum
from typing import Any, Dict, Optional

from ducktape.services.service import Service
from ducktape.tests.test import TestContext
from pyiceberg.catalog import load_catalog

from rptest.context import cloud_storage


class CatalogType(str, Enum):
    REST_JDBC = "rest_jdbc"
    REST_HADOOP = "rest_hadoop"
    POLARIS = "polaris"
    NESSIE = "nessie"
    DATABRICKS_UNITY = "databricks_unity"
    BIGLAKE = "biglake"


def catalog_type_to_config_string(catalog_type: CatalogType) -> str:
    """
    Query engines expect iceberg.catalog.type to be a configured property.
    This value is dictated by the catalog and not the implementation,
    which is why e.g. both JdbcCatalog and HadoopCatalog map to 'rest'.
    """
    if catalog_type in [CatalogType.REST_JDBC, CatalogType.REST_HADOOP]:
        return "rest"
    elif catalog_type == CatalogType.POLARIS:
        return "polaris"
    elif catalog_type == CatalogType.NESSIE:
        return "nessie"

    raise ValueError(f"Unsupported catalog type: {catalog_type}")


class CatalogService(abc.ABC, Service):
    DEFAULT_WAREHOUSE_NAME = "redpanda-iceberg-catalog"

    def __init__(
        self,
        ctx: TestContext,
        cloud_storage_bucket: str,
        warehouse_name: str = DEFAULT_WAREHOUSE_NAME,
        **kwargs: Any,
    ):
        super().__init__(ctx, **kwargs)
        self.dedicated_nodes = ctx.globals.get("dedicated_nodes", False)
        self.credentials = cloud_storage.Credentials.from_context(ctx)

        self.cloud_storage_bucket = cloud_storage_bucket
        self.warehouse_name = warehouse_name
        self._catalog_url = None

    @abc.abstractmethod
    def catalog_type(self) -> CatalogType: ...

    @property
    @abc.abstractmethod
    def iceberg_rest_url(self) -> str: ...

    @property
    @abc.abstractmethod
    def iceberg_rest_port(self) -> int: ...

    @property
    def vendor_api_url(self) -> str:
        """
        Some services (e.g. Nessie) may expose a vendor-specific API endpoint
        that is distinct from the Iceberg REST API which can then be leveraged
        by custom clients (e.g. Spark extensions).
        """
        raise NotImplementedError(
            f"Vendor API URL not implemented for catalog type: {self.catalog_type}"
        )

    def compute_warehouse_path(self):
        """
        Provides the physical location of the Iceberg warehouse in storage.
        This function can be overridden in child classes of CatalogService
        if needed for a specific catalog implementation.
        """
        if isinstance(self.credentials, cloud_storage.S3Credentials) or isinstance(
            self.credentials, cloud_storage.AWSInstanceMetadataCredentials
        ):
            s3_prefix = "s3"
            self.cloud_storage_warehouse = (
                f"{s3_prefix}://{self.cloud_storage_bucket}/{self.warehouse_name}"
            )
        elif isinstance(self.credentials, cloud_storage.GCPInstanceMetadataCredentials):
            self.cloud_storage_warehouse = (
                f"gs://{self.cloud_storage_bucket}/{self.warehouse_name}"
            )
        elif isinstance(self.credentials, cloud_storage.ABSSharedKeyCredentials):
            self.cloud_storage_warehouse = f"abfss://{self.cloud_storage_bucket}@{self.credentials.endpoint}/{self.warehouse_name}"
        else:
            raise ValueError(f"Unsupported credential type: {type(self.credentials)}")

    def client(self, catalog_name: str = "default"):
        conf: dict[str, str | None] = {}
        conf["uri"] = self.iceberg_rest_url
        conf["warehouse"] = self.cloud_storage_warehouse

        if isinstance(self.credentials, cloud_storage.S3Credentials):
            conf["s3.endpoint"] = self.credentials.endpoint
            conf["s3.access-key-id"] = self.credentials.access_key
            conf["s3.secret-access-key"] = self.credentials.secret_key
            conf["s3.region"] = self.credentials.region
        elif isinstance(self.credentials, cloud_storage.AWSInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials, cloud_storage.GCPInstanceMetadataCredentials):
            pass
        elif isinstance(self.credentials, cloud_storage.ABSSharedKeyCredentials):
            # Legancy pyiceberg https://github.com/apache/iceberg-python/issues/866
            conf["adlfs.account-name"] = self.credentials.account_name
            conf["adlfs.account-key"] = self.credentials.account_key
            # Modern pyiceberg https://github.com/apache/iceberg-python/issues/866
            conf["adls.account-name"] = self.credentials.account_name
            conf["adls.account-key"] = self.credentials.account_key
        else:
            raise ValueError(f"Unsupported credential type: {type(self.credentials)}")

        # Subclasses may need to configure the client further.
        self._configure_client(conf)

        return load_catalog(catalog_name, **conf)

    def _configure_client(self, conf: Dict[str, Optional[str]]):
        """
        Delegate to subclasses more advanced client configuration like
        authentication. Mutate conf in place.
        """
        pass

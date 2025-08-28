# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from typing import Dict, Optional

from rptest.context.databricks import (
    DatabricksContext,
    OauthCredentials,
    PatCredentials,
)
from rptest.services.catalog_service import CatalogService, CatalogType
from rptest.services.databricks_workspace import DatabricksCatalogInfo


class DatabricksUnity(CatalogService):
    def __init__(
        self, ctx, *, cloud_storage_bucket: str, catalog: DatabricksCatalogInfo
    ):
        super().__init__(
            ctx,
            cloud_storage_bucket,
            num_nodes=0,
        )

        self._databricks_context = DatabricksContext.from_context(ctx)
        self.warehouse_name = catalog.name
        self.compute_warehouse_path()

    def catalog_type(self) -> CatalogType:
        return CatalogType.DATABRICKS_UNITY

    @property
    def iceberg_rest_url(self) -> str:
        return self._databricks_context.iceberg_rest_url

    @property
    def iceberg_rest_port(self) -> int:
        raise NotImplementedError("Databricks Unity does not expose a port")

    def _configure_client(self, conf: Dict[str, Optional[str]]) -> None:
        if isinstance(self._databricks_context.credentials, PatCredentials):
            conf["token"] = self._databricks_context.credentials.token
        elif isinstance(self._databricks_context.credentials, OauthCredentials):
            conf["credential"] = (
                f"{self._databricks_context.credentials.client_id}:{self._databricks_context.credentials.client_secret}"
            )
            conf["scope"] = "all-apis"
            conf["oauth2-server-uri"] = (
                f"{self._databricks_context.workspace_url}/oidc/v1/token"
            )
        else:
            raise ValueError(
                f"Unsupported credentials type for Databricks context for {type(self._databricks_context.credentials)}"
            )

        # TODO: Understand why CatalogService sets warehouse to a cloud storage
        # uri like s3://bucket/... and it works for all the tests but not
        # with databricks. We should aim to unify this otherwise it looks like
        # a hack or bug somewhere.
        conf["warehouse"] = self.warehouse_name
